import ast
import unittest

from alertingest.schema_registry import SchemaRegistryClient


class TestSchemaRegistryClient(unittest.TestCase):

    def test_get_raw_schema(self):
        """Test that the raw schema retreived from the schema registry is
        the one requested.
        """
        self.schema_registry = SchemaRegistryClient(
            address="https://usdf-alert-schemas-dev.slac.stanford.edu"
        )
        schema = self.schema_registry.get_raw_schema(300)
        self.assertEqual(ast.literal_eval(schema)["namespace"], "lsst.v3_0")

    def test_get_raw_schema_fail(self):
        """Test that the correct error is raised if the schema requested
        is not in the schema registry."""
        self.schema_registry = SchemaRegistryClient(
            address="https://usdf-alert-schemas-dev.slac.stanford.edu"
        )
        with self.assertRaises(KeyError):
            self.schema_registry.get_raw_schema(1)

    def test_get_schema_decoder(self):
        """Check that the schema decoder"""
        self.schema_registry = SchemaRegistryClient(
            address="https://usdf-alert-schemas-dev.slac.stanford.edu"
        )
        self.schema_registry.get_schema_decoder(300)
        self.assertIn(300, self.schema_registry._cached_decoders)


if __name__ == "__main__":
    unittest.main()
