from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
from jinja2 import Environment, FileSystemLoader

from airflow.utils.email import send_email_smtp

from utils.config.dag_params import (
    get_dag_status,
    get_execution_date,
    get_feature_flags,
)
from _types.dags import DagStatus
from enums.mail import MailPriority, MailStatus
from utils.config.tasks import get_list_contact, get_list_documentation
from utils.config.vars import (
    FF_MAIL_DISABLED_MSG,
    get_root_folder,
    DEFAULT_SMTP_CONN_ID,
    DEFAULT_MAIL_CC,
    paris_tz,
)


default_mail_config = {
    MailStatus.START: {
        "template_path": "pipeline_start.html",
        "subject": "[DEBUT] - Lancement de la pipeline",
        "priority": MailPriority.NORMAL.value,
    },
    MailStatus.SUCCESS: {
        "template_path": "pipeline_end_success.html",
        "subject": "[FIN] - Fin de la pipeline",
        "priority": MailPriority.NORMAL.value,
    },
    MailStatus.ERROR: {
        "template_path": "pipeline_end_error.html",
        "subject": "[ECHEC] - Une erreur est survenue dans la pipeline",
        "priority": MailPriority.HIGH.value,
    },
    MailStatus.SKIP: {
        "template_path": "",
        "subject": "",
        "priority": MailPriority.NORMAL.value,
    },
    MailStatus.WARNING: {
        "template_path": "",
        "subject": "",
        "priority": MailPriority.NORMAL.value,
    },
    MailStatus.INFO: {
        "template_path": "logs_recap.html",
        "subject": "Notification - Nettoyage des logs",
        "priority": MailPriority.LOW.value,
    },
}


@dataclass
class MailMessage:
    to: list[str]
    mail_status: Optional[MailStatus] = None
    subject: Optional[str] = None
    template_dir: Path | None = None
    html_content: Optional[str] = None
    template_parameters: dict = field(default_factory=dict)
    cc: Optional[list[str]] = None
    bcc: Optional[list[str]] = None
    from_email: Optional[str] = None
    files: Optional[list[str]] = None
    custom_headers: Optional[dict[str, Any]] = None

    def __post_init__(self) -> None:
        # Cas 1: mail_status est précisé -> génération automatique du template
        if self.mail_status is not None:
            config = default_mail_config.get(self.mail_status)

            if not config or not config.get("template_path"):
                raise ValueError(
                    f"Aucun template configuré pour le statut {self.mail_status.value}"
                )

            self.html_content = render_template(
                template_dir=self.template_dir,
                template_name=config["template_path"],
                template_parameters=self.template_parameters,
            )
            self.subject = config["subject"]
            self.custom_headers = {"X-Priority": str(config["priority"])}

        # Cas 2: mail_status n'est pas précisé -> html_content doit être fourni
        elif self.html_content is None or self.subject is None:
            raise ValueError(
                "html_content et subject doivent être fournis si mail_status n'est pas précisé"
            )


def render_template(
    template_name: str,
    template_parameters: Mapping[str, str],
    template_dir: Path | None = None,
) -> str:
    if template_dir is None:
        root_folder = get_root_folder()
        template_dir = Path(
            root_folder,
            "infra",
            "mails",
            "templates",
        )

    # Set up the Jinja environment with a loader pointing to the templates directory
    env = Environment(loader=FileSystemLoader(searchpath=str(template_dir)))

    # Load the template by name (just the filename, not full path)
    template = env.get_template(name=template_name)

    # Render with context
    return template.render(**template_parameters)


def send_mail(mail_message: MailMessage, conn_id: str = DEFAULT_SMTP_CONN_ID) -> None:

    if not mail_message.subject or not mail_message.html_content:
        raise ValueError("html_content et subject undefined ...")

    send_email_smtp(
        conn_id=conn_id,
        to=mail_message.to,
        cc=mail_message.cc,
        bcc=mail_message.bcc,
        subject=mail_message.subject,
        html_content=mail_message.html_content,
        files=mail_message.files,
        custom_headers=mail_message.custom_headers,
    )


def create_send_mail_callback(mail_status: MailStatus) -> Callable:
    """
    Create an Airflow callback function for mail notifications.

    Args:
        mail_status: Status to send notification for

    Returns:
        Callable: Callback function for Airflow
    """

    def _callback(context: dict[str, Any]) -> None:
        # If debug mode is ON, we don't want to send any mail
        mail_enable = get_feature_flags(context=context).mail
        dag_status = get_dag_status(context=context)

        if dag_status == DagStatus.DEV:
            print("Dag status parameter is set to DEV -> skipping this task ...")
            return

        if not mail_enable:
            print(FF_MAIL_DISABLED_MSG)
            return

        projet_contact = get_list_contact(context=context)
        mail_to = [
            contact.contact_mail for contact in projet_contact if contact.is_generic
        ]
        mail_cc = [
            contact.contact_mail for contact in projet_contact if not contact.is_generic
        ]

        projet_docs = get_list_documentation(context=context)
        doc_pipeline = [
            doc.lien for doc in projet_docs if doc.type_documentation == "pipeline"
        ]
        doc_data = [doc.lien for doc in projet_docs if doc.type_documentation == "data"]
        execution_date = get_execution_date(context=context)

        if isinstance(mail_cc, list):
            mail_cc.extend(DEFAULT_MAIL_CC)
        if mail_cc is None:
            mail_cc = DEFAULT_MAIL_CC

        mail_message = MailMessage(
            mail_status=mail_status,
            to=mail_to,
            cc=mail_cc,
            template_parameters={
                "dag_name": context["dag"].dag_id,
                "dag_statut": mail_status.value,
                "start_date": execution_date.replace(tzinfo=paris_tz).strftime(
                    format="%d-%m-%Y %H:%M:%S"
                ),
                "link_doc_pipeline": ";".join(doc_pipeline),
                "link_doc_donnees": ";".join(doc_data),
            },
        )

        send_mail(mail_message=mail_message)

    return _callback
