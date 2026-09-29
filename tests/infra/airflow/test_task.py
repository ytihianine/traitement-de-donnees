from collections.abc import Callable
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
from modules.domain.pipeline.model import ExecutionOptions, PipelineDescriptor
from modules.domain.pipeline.output import JsonOutput
from modules.infra.airflow import task as task_module


@pytest.fixture
def use_plain_task_decorator(monkeypatch: pytest.MonkeyPatch) -> None:
    def plain_task(**kwargs):
        def decorate(function):
            return function

        return decorate

    monkeypatch.setattr(task_module, "task", plain_task)


def create_pipeline(
    operation: Callable[[], object | None],
    add_metadata: bool = False,
) -> PipelineDescriptor:
    return cast(
        "PipelineDescriptor",
        SimpleNamespace(
            input_datasets=(),
            output_dataset=SimpleNamespace(name="output_dataset"),
            operation=operation,
            use_input_results_as_operation_args=False,
            add_metadata=add_metadata,
        ),
    )


def test_task_does_not_export_when_operation_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    use_plain_task_decorator: None,
) -> None:
    def operation() -> None:
        return None

    provider_factory = Mock()
    monkeypatch.setattr(task_module, "create_dataset_location_provider", provider_factory)
    output_adapter_registry = Mock()

    created_task = task_module.create_task(
        pipeline=create_pipeline(operation=operation),
        execution_options=ExecutionOptions(),
        dag_repo=Mock(get_project_name=Mock(return_value="project")),
        projet_repo=Mock(),
        dataset_context_repo=Mock(),
        output_adapter_registry=output_adapter_registry,
    )

    created_task()

    provider_factory.assert_not_called()
    output_adapter_registry.get_adapter.assert_not_called()


def test_task_exports_json_result_with_selected_adapter(
    monkeypatch: pytest.MonkeyPatch,
    use_plain_task_decorator: None,
) -> None:
    result = JsonOutput({"foo": "bar"})

    def operation() -> JsonOutput:
        return result

    output_location = SimpleNamespace(validate_location="exports/output.json")
    provider = Mock()
    adapter = Mock()
    output_adapter_registry = Mock(get_adapter=Mock(return_value=adapter))
    monkeypatch.setattr(task_module, "create_dataset_location_provider", Mock(return_value=provider))
    df_info = Mock()
    monkeypatch.setattr(task_module, "df_info", df_info)

    created_task = task_module.create_task(
        pipeline=create_pipeline(operation=operation),
        execution_options=ExecutionOptions(),
        dag_repo=Mock(get_project_name=Mock(return_value="project")),
        projet_repo=Mock(),
        dataset_context_repo=Mock(get=Mock(return_value=SimpleNamespace(dest_location=output_location))),
        output_adapter_registry=output_adapter_registry,
    )

    created_task()

    output_adapter_registry.get_adapter.assert_called_once_with(result)
    adapter.write.assert_called_once_with(
        output=result,
        provider=provider,
        location="exports/output.json",
    )
    df_info.assert_not_called()


def test_task_rejects_metadata_for_non_dataframe_result(
    use_plain_task_decorator: None,
) -> None:
    def operation() -> JsonOutput:
        return JsonOutput({"foo": "bar"})

    created_task = task_module.create_task(
        pipeline=create_pipeline(operation=operation, add_metadata=True),
        execution_options=ExecutionOptions(),
        dag_repo=Mock(get_project_name=Mock(return_value="project")),
        projet_repo=Mock(),
        dataset_context_repo=Mock(),
        output_adapter_registry=Mock(),
    )

    with pytest.raises(TypeError, match="add_metadata is only supported for DataFrame results"):
        created_task()
