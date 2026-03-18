import os
import re
import warnings
from typing import Any

import yaml


def _resolve_includes(
    content: str,
    base_dir: str,
    visited: set[str] | None = None,
) -> str:
    """
    Recursively resolves !include directives in YAML content.

    The !include directive inlines the content from the referenced file at the same
    indentation level, allowing merging with sibling keys.

    Examples:
      parent:
        !include file.yaml    # Content of file.yaml is inlined here
        extra_key: value      # This merges with included content

    :param content: The YAML content as a string.
    :param base_dir: The base directory for resolving relative paths.
    :param visited: Set of already visited files to prevent circular includes.
    :return: The content with all includes resolved.
    :raises ValueError: If circular includes are detected.
    :raises FileNotFoundError: If an included file cannot be found.
    :raises IOError: If an included file cannot be read.
    """
    if visited is None:
        visited = set()

    # Pattern to match !include directives (standalone or after key:)
    # Key names may contain hyphens (e.g. pre-steps, plot-configs) in addition
    # to standard word characters.
    pattern = r"^(\s*)(?:([\w-]+):\s*)?!include\s+(.+)$"

    lines = content.split("\n")
    processed_lines = []

    for line in lines:
        match = re.match(pattern, line)
        if match:
            indent = match.group(1)
            key_part = match.group(2)  # Just the key name (no colon)
            raw_path = match.group(3).split("#")[0].strip()  # Remove comments and strip whitespace

            # Detect list form: !include [file_a.yaml, file_b.yaml]
            if raw_path.startswith("[") and raw_path.endswith("]"):
                include_paths = [p.strip() for p in raw_path[1:-1].split(",")]
            else:
                include_paths = [raw_path]

            merged_resolved = ""
            for include_path in include_paths:
                full_path = os.path.abspath(os.path.join(base_dir, include_path))

                # Check for circular includes
                if full_path in visited:
                    raise ValueError(f"Circular include detected: {full_path}")

                visited.add(full_path)

                try:
                    with open(full_path, "r") as inc_file:
                        included_content = inc_file.read()
                except FileNotFoundError as e:
                    raise FileNotFoundError(
                        f"Cannot find included file '{include_path}' (resolved to: {full_path})"
                    ) from e
                except IOError as e:
                    raise IOError(
                        f"Cannot read included file '{include_path}' (resolved to: {full_path})"
                    ) from e

                included_dir = os.path.dirname(full_path)
                merged_resolved += _resolve_includes(included_content, included_dir, visited)
                if not merged_resolved.endswith("\n"):
                    merged_resolved += "\n"

                visited.remove(full_path)

            resolved_content = merged_resolved.rstrip("\n")

            if key_part:
                # Case: key: !include file.yaml
                # Add the key line, then indent the content
                processed_lines.append(f"{indent}{key_part}:")
                nested_indent = indent + "  "
                indented_lines = [
                    nested_indent + line if line.strip() else line
                    for line in resolved_content.split("\n")
                ]
                processed_lines.extend(indented_lines)
            else:
                # Case: !include file.yaml (standalone)
                # Inline the content at the same indentation level
                indented_lines = [
                    indent + line if line.strip() else line for line in resolved_content.split("\n")
                ]
                processed_lines.extend(indented_lines)
        else:
            processed_lines.append(line)

    return "\n".join(processed_lines)


class Config:
    """
    Configuration class to handle loading and setting up configurations from a YAML file.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initializes the Config object.

        :param config_path: The path to the configuration file.
        :type config_path: str
        """
        self.path = os.path.abspath(config_path)
        self._setup()

    def _setup(self) -> None:
        """
        Sets up the configuration by loading the config file and setting attributes.
        """
        config_dict = self._load_config(self.path)
        if not config_dict:
            config_dict = {}
        for key in config_dict.keys():
            setattr(self, key, config_dict[key])
            # Evaluate types of opt_args
            if key == "opt_args":
                for subkey in self.opt_args.keys():
                    try:
                        self.opt_args[subkey]["type"] = eval(self.opt_args[subkey]["type"])
                    except KeyError:
                        continue

    def _delete_existing_attributes(self) -> None:
        """
        Deletes existing attributes of the Config object.
        """
        for attr in list(self.__dict__.keys()):
            if attr not in ["path"]:
                delattr(self, attr)

    def change_config_file(self, config_path: str = "./run_config.yaml") -> None:
        """
        Changes the configuration file to a new path.

        :param config_path: The relative path to the new config file. Default is "./run_config.yaml"
        :type config_path: str
        """
        try:
            self._delete_existing_attributes()
            self.path = os.path.abspath(config_path)
            self._setup()
        except Exception as e:
            warnings.warn(
                f"Error changing configuration file: {e}. Using the default configuration file."
            )

    @staticmethod
    def _load_config(config_path: str) -> dict[str, Any]:
        """
        Loads the configuration from a YAML file supporting !include directives.

        All !include directives (both top-level and nested) are resolved before
        parsing the YAML, with paths relative to the file containing the directive.

        :param config_path: The path to the configuration file.
        :type config_path: str
        :return: The loaded configuration dictionary.
        :rtype: dict
        :raises FileNotFoundError: If the config file or any included file is not found.
        :raises yaml.YAMLError: If there's an error parsing the YAML.
        :raises ValueError: If circular includes are detected.
        """
        try:
            with open(config_path, "r") as file:
                content = file.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Configuration file not found: {config_path}") from e
        except IOError as e:
            raise IOError(f"Cannot read configuration file: {config_path}") from e

        # Recursively resolve all !include directives
        try:
            resolved_content = _resolve_includes(content, os.path.dirname(config_path))
        except (FileNotFoundError, ValueError, IOError) as e:
            raise type(e)(f"Error processing includes in {config_path}: {e}") from e

        # Parse the final YAML with standard loader
        try:
            config = yaml.safe_load(resolved_content)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML configuration from {config_path}: {e}") from e

        return config if config else {}


class _LazyRunConfig:
    """Lazy singleton proxy for the runtime config.

    The underlying :class:`Config` instance is created on first access,
    avoiding eager file I/O at import time.
    """

    def __init__(self) -> None:
        self._instance: Config | None = None

    def _get_instance(self) -> Config:
        if self._instance is None:
            raise RuntimeError(
                "RUN_CONFIG is not initialized. Set it first with "
                "ydana.base.config.set_run_config(<config_path_or_Config>) or "
                "run via CLI with --config-file."
            )
        return self._instance

    def set(self, config: Config | str) -> Config:
        """Set/replace the singleton instance.

        Parameters
        ----------
        config : Config or str
            Existing config instance or path to a YAML config file.
        """
        if isinstance(config, Config):
            self._instance = config
        else:
            self._instance = Config(os.path.abspath(config))
        return self._instance

    def reset(self) -> None:
        """Drop current instance so it is recreated on next access."""
        self._instance = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._get_instance(), name)

    def __repr__(self) -> str:
        instance = self.__dict__.get("_instance")
        if instance is None:
            return "_LazyRunConfig(uninitialized)"
        return f"_LazyRunConfig({instance!r})"


def get_run_config() -> Config:
    """Return the active singleton run config instance."""
    return RUN_CONFIG._get_instance()


def set_run_config(config: Config | str) -> Config:
    """Replace the active singleton run config instance.

    Parameters
    ----------
    config : Config or str
        Existing config instance or path to a YAML config file.
    """
    return RUN_CONFIG.set(config)


# ------- create lazy RUN_CONFIG singleton proxy -------
RUN_CONFIG = _LazyRunConfig()
