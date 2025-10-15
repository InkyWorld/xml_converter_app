from pathlib import Path
from typing import Optional
from xml.etree.ElementTree import ParseError

import xmlschema

from src.logger_config import app_logger


class XmlSchemaValidator:
    """
    Validates XML files against a pre-loaded and compiled XSD schema
    using the xmlschema library.

    This class is designed for efficiency and reusability. The XSD schema
    is parsed and compiled once during initialization. The validate() method
    can then be called multiple times for different XML files.
    """

    def __init__(self, schema_path: Path):
        """
        Initializes the validator and pre-loads the XSD schema.

        Args:
            schema_path (Path): The file path to the XSD schema file.
        """
        self.schema_path = schema_path
        self.schema: Optional[xmlschema.XMLSchema11] = self._load_schema()

    def _load_schema(self) -> Optional[xmlschema.XMLSchema11]:
        """
        Parses and compiles the XSD schema file.

        This private method is called once during initialization.
        It handles schema file existence and parsing errors.

        Returns:
            An xmlschema.XMLSchema object if successful, otherwise None.
        """
        if not self.schema_path.is_file():
            app_logger.error(
                f"\n[✗] CRITICAL ERROR: Schema file not found at '{self.schema_path}'"
            )
            return None
        try:
            return xmlschema.XMLSchema11(str(self.schema_path))
        except xmlschema.XMLSchemaException as e:
            app_logger.error(
                f"\n[✗] CRITICAL ERROR: Failed to parse the XSD schema. Details: {e}"
            )
            return None

    def validate(self, xml_path: Path) -> bool:
        """
        Executes the validation process for a given XML file.

        This method can be called multiple times to validate different XML files
        against the schema that was loaded during the object's initialization.

        Args:
            xml_path (Path): The path to the XML file to be validated.

        Returns:
            True if the XML file is valid, False otherwise.
        """
        # Check if the schema was loaded successfully
        if self.schema is None:
            app_logger.error("[✗] VALIDATION SKIPPED: Schema was not loaded correctly.")
            return False

        # Check if the XML file exists
        if not xml_path.is_file():
            app_logger.error(f"[✗] VALIDATION FAILED: XML file not found at '{xml_path}'")
            return False

        try:
            # Use iter_errors to get all validation errors
            validation_errors = list(self.schema.iter_errors(str(xml_path)))

            if not validation_errors:
                app_logger.info(f"[✓] SUCCESS: '{xml_path.name}' is valid.")
                return True
            else:
                app_logger.error(
                    f"[✗] VALIDATION FAILED: '{xml_path.name}' does not conform to the schema."
                )
                app_logger.error("Errors found:")
                for error in validation_errors:
                    location = f"Line {error.sourceline}" if hasattr(error, 'sourceline') and error.sourceline is not None else "Location unknown"
                    app_logger.error(f"  - {location}: {error.reason} (Path: {error.path})")
                return False

        except ParseError as e:
            app_logger.error(
                f"[✗] VALIDATION FAILED: '{xml_path.name}' has syntax errors and cannot be parsed. Details: {e}"
            )
            return False

        except Exception as e:
            app_logger.error(f"[✗] VALIDATION FAILED: An unexpected error occurred. Details: {e}")
            return False