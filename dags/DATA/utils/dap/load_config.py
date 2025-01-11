import glob
import pathlib
import re
import yaml
from typing import Generator, Tuple

from DATA.models.dap.config_models import PipelineConfiguration


class ConfigFile:

    @staticmethod
    def load_single_configuration(config_yaml_path: str) -> Tuple[str, PipelineConfiguration]:
        """Loads and converts a single sFTP configuration.

        Args:
             config_yaml_path: the path to a single YAML file.

        Returns:
            an PipelineConfiguration Object.
        """
        if config_yaml_path is None:
            raise ValueError('no YAML file provided for loading.')

        file_name = pathlib.Path(config_yaml_path).name
        config_name = re.sub(r"[^\w-]+", '_', file_name)
        
        return config_name, PipelineConfiguration.from_dict(yaml.safe_load(open(config_yaml_path)))

    @staticmethod
    def load_configurations(file_pattern: str) -> Generator[Tuple[str, PipelineConfiguration], None, None]:
        """Loads the sFTP transfer and raw transformation configurations from YAML files.

        Args:
            file_pattern: absolute path pattern to locate files. e.g. /data/raw_*.yaml

        Returns:
            A generator object providing an iterator for each identified file.

        Raises:
            ValueError: if empty of None file_pattern is provided.
        """
        if not file_pattern:
            raise ValueError('empty or No file pattern provided.')

        for next_file in glob.glob(file_pattern):
            yield ConfigFile.load_single_configuration(next_file)
