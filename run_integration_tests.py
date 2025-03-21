#!/usr/bin/env python3
"""Run all integration tests for the GTFS-RT project."""

import unittest
import sys


if __name__ == "__main__":
    # Load all tests from the integration tests directory
    test_suite = unittest.defaultTestLoader.discover('tests/integration')
    
    # Run the tests
    result = unittest.TextTestRunner(verbosity=2).run(test_suite)
    
    # Return non-zero exit code if tests failed
    sys.exit(not result.wasSuccessful()) 