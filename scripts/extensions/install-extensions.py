import subprocess
import os
import json

DIR = os.path.dirname(os.path.realpath(__file__))
EXT_FILE = f"{DIR}/extensions.json"


def download_and_install_vsix(publisher: str, extension: str, version: str) -> None:
    """Download and install a VSIX extension."""
    url = f"https://marketplace.visualstudio.com/_apis/public/gallery/publishers/{publisher}/vsextensions/{extension}/{version}/vspackage"  # noqa
    vsix_gz = f"{extension}.vsix.gz"
    vsix = f"{extension}.vsix"

    # Download the VSIX package
    subprocess.run(
        ["wget", "--retry-on-http-error=429", url, "-O", vsix_gz], check=True
    )

    # Decompress the file
    subprocess.run(["gzip", "-d", vsix_gz], check=True)

    # Install the extension
    subprocess.run(["code-server", "--install-extension", vsix], check=True)

    # Clean up
    os.remove(path=vsix)


def apply_settings(settings: dict, settings_path: str | None = None) -> None:
    """Apply VS Code settings."""
    if settings_path is None:
        # Default VS Code settings path for code-server
        settings_path = os.path.expanduser(
            "~/.local/share/code-server/User/settings.json"
        )

    # Create directory if it doesn't exist
    os.makedirs(name=os.path.dirname(settings_path), exist_ok=True)

    # Load existing settings if file exists
    existing_settings = {}
    if os.path.exists(path=settings_path):
        with open(file=settings_path, mode="r") as f:
            try:
                existing_settings = json.load(fp=f)
            except json.JSONDecodeError:
                existing_settings = {}

    # Merge new settings with existing ones
    existing_settings.update(settings)

    # Write updated settings
    with open(file=settings_path, mode="w") as f:
        json.dump(obj=existing_settings, fp=f, indent=4)

    print(f"Settings applied to {settings_path}")


def main() -> None:
    # Python extensions

    with open(file=EXT_FILE, mode="r") as file:
        extensions = json.load(fp=file)

    print("Installing Python extensions...")
    for ext in extensions:
        print(f"Installing {ext['name']}@{ext['version']}...")
        download_and_install_vsix(
            publisher=ext["publisher"], extension=ext["name"], version=ext["version"]
        )

    print("\nAll extensions installed successfully!")

    # Extension settings
    extension_settings = {
        "workbench.sash.hoverDelay": 0,
        "workbench.hover.delay": 0,
        "security.workspace.trust.enabled": False,
        "security.workspace.trust.startupPrompt": "never",
        "terminal.integrated.inheritEnv": False,
        "workbench.colorTheme": "Default Dark Modern",
        "editor.minimap.enabled": False,
        "basedpyright.analysis.diagnosticMode": "workspace",
        "basedpyright.analysis.typeCheckingMode": "standard",
        "basedpyright.analysis.inlayHints.variableTypes": False,
        "flake8.args": ["--max-line-length=120"],
    }
    apply_settings(settings=extension_settings)


if __name__ == "__main__":
    main()
