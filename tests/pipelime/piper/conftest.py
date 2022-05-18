from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def piper_commands(data_folder):

    base_path = Path(data_folder) / "piper" / "piper_commands"

    def build_python_command(command: str):
        return f"python {str(base_path / command)}"

    return {
        "fake_detector": {
            "command": build_python_command("fake_detector.py"),
            "valid": True,
            "exception": None,
        },
    }


@pytest.fixture(scope="session")
def piper_dags(data_folder, piper_commands, tmp_path_factory: pytest.TempPathFactory):

    base_path = Path(data_folder) / "piper" / "dags"

    return {
        "complex": {
            "folder": base_path / "complex",
            "valid": True,
            "exception": None,
            "graph": True,
            "executable": {
                # If success is TRUE a "final_validation.py" file is needed in the
                # dag folder
                "success": True,
                "executable_placeholders": {
                    "CUSTOM_COMMAND": piper_commands["fake_detector"]["command"],
                    "OUTPUT_FOLDER": tmp_path_factory.mktemp("output_folder"),
                    "FINAL_FOLDER": tmp_path_factory.mktemp("final_folder"),
                    "SIZE": 10,
                    "SCHEMA_GENERATED": base_path
                    / "complex"
                    / "schema_generated.schema",
                    "SCHEMA_REMAPPED": base_path / "complex" / "schema_remapped.schema",
                    "SCHEMA_DETECTED": base_path / "complex" / "schema_detected.schema",
                },
                "exception": None,
            },
        },
        # "validation_error": {
        #     "folder": base_path / "validation_error",
        #     "valid": True,
        #     "exception": None,
        #     "graph": True,
        #     "executable": {
        #         # If success is TRUE a "final_validation.py" file is needed in the
        #         # dag folder
        #         "success": False,
        #         "executable_placeholders": {
        #             "OUTPUT_FOLDER": tmp_path_factory.mktemp("output_folder"),
        #             "SIZE": 10,
        #             "SCHEMA_GENERATED": base_path
        #             / "validation_error"
        #             / "schema_generated.schema",
        #         },
        #         "exception": SampleSchema.ValidationError,
        #     },
        # },
        # "validation_error_noschema_file": {
        #     "folder": base_path / "validation_error_noschema_file",
        #     "valid": True,
        #     "exception": None,
        #     "graph": True,
        #     "executable": {
        #         # If success is TRUE a "final_validation.py" file is needed in the
        #         # dag folder
        #         "success": False,
        #         "executable_placeholders": {
        #             "OUTPUT_FOLDER": tmp_path_factory.mktemp("output_folder"),
        #             "SIZE": 10,
        #             "SCHEMA_GENERATED": base_path
        #             / "validation_error_noschema_file"
        #             / "schema_generated.schema",
        #         },
        #         "exception": SampleSchema.ValidationError,
        #     },
        # },
        # "no_params": {
        #     "folder": base_path / "no_params",
        #     "valid": True,
        #     "exception": None,
        #     "graph": False,
        # },
        # "invalid_params": {
        #     "folder": base_path / "invalid_params",
        #     "valid": False,
        #     "exception": KeyError,
        #     "graph": False,
        # },
        # "invalid_nodes": {
        #     "folder": base_path / "invalid_nodes",
        #     "valid": False,
        #     "exception": pydantic.ValidationError,
        #     "graph": False,
        # },
        # "invalid_node_foreach": {
        #     "folder": base_path / "invalid_node_foreach",
        #     "valid": False,
        #     "exception": KeyError,
        #     "graph": False,
        # },
        # "invalid_node_foreach_do_content": {
        #     "folder": base_path / "invalid_node_foreach_do_content",
        #     "valid": False,
        #     "exception": TypeError,
        #     "graph": False,
        # },
        # "invalid_arg_foreach": {
        #     "folder": base_path / "invalid_arg_foreach",
        #     "valid": False,
        #     "exception": KeyError,
        #     "graph": False,
        # },
        # "invalid_arg_foreach_content": {
        #     "folder": base_path / "invalid_arg_foreach_content",
        #     "valid": False,
        #     "exception": TypeError,
        #     "graph": False,
        # },
        # "single_operation_with_multi_branches": {
        #     "folder": base_path / "single_operation_with_multi_branches",
        #     "valid": True,
        #     "exception": None,
        #     "graph": True,
        #     "executable": {
        #         # If success is TRUE a "final_validation.py" file is needed in the
        #         # dag folder
        #         "success": True,
        #         "executable_placeholders": {
        #             "OUTPUT_FOLDER": tmp_path_factory.mktemp("output_folder"),
        #         },
        #         "exception": None,
        #     },
        # },
    }
