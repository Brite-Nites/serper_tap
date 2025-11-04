"""Tests for timing utilities."""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.utils.timing import timing


class TestTiming:
    """Test timing context manager."""

    @patch("src.utils.timing.get_run_logger")
    def test_timing_logs_operation(self, mock_logger):
        """Test that timing logs the operation duration."""
        mock_logger.return_value = MagicMock()

        with timing("test operation"):
            time.sleep(0.01)  # Sleep for 10ms

        # Check that info was called
        assert mock_logger.return_value.info.called
        call_args = str(mock_logger.return_value.info.call_args)
        assert "test operation" in call_args
        assert "ms" in call_args or "s" in call_args

    @patch("src.utils.timing.get_run_logger")
    def test_timing_returns_elapsed_ms(self, mock_logger):
        """Test that timing_data dict is updated with elapsed_ms."""
        mock_logger.return_value = MagicMock()

        with timing("test operation") as timing_data:
            time.sleep(0.01)
            # elapsed_ms not set yet (still in context)
            assert timing_data["elapsed_ms"] == 0

        # After context, elapsed_ms should be set
        assert timing_data["elapsed_ms"] > 0
        assert timing_data["elapsed_ms"] >= 10  # At least 10ms

    @patch("src.utils.timing.get_run_logger")
    def test_timing_with_threshold(self, mock_logger):
        """Test that timing respects log_threshold_ms."""
        mock_logger.return_value = MagicMock()

        # Operation takes ~10ms, threshold is 100ms - should NOT log
        with timing("fast operation", log_threshold_ms=100):
            time.sleep(0.01)

        # Should not have logged
        assert not mock_logger.return_value.info.called

    @patch("src.utils.timing.get_run_logger")
    def test_timing_exceeds_threshold(self, mock_logger):
        """Test that timing logs when exceeding threshold."""
        mock_logger.return_value = MagicMock()

        # Operation takes ~20ms, threshold is 10ms - should log
        with timing("slow operation", log_threshold_ms=10):
            time.sleep(0.02)

        # Should have logged
        assert mock_logger.return_value.info.called

    @patch("src.utils.timing.get_run_logger")
    def test_timing_formats_milliseconds(self, mock_logger):
        """Test that timing formats sub-second durations as milliseconds."""
        mock_logger.return_value = MagicMock()

        with timing("fast operation"):
            time.sleep(0.01)  # 10ms

        call_args = str(mock_logger.return_value.info.call_args)
        assert "ms" in call_args
        assert "s" not in call_args.replace("ms", "")  # No standalone 's'

    @patch("src.utils.timing.get_run_logger")
    def test_timing_formats_seconds(self, mock_logger):
        """Test that timing formats >1s durations as seconds."""
        mock_logger.return_value = MagicMock()

        with timing("slow operation"):
            time.sleep(1.1)  # 1.1 seconds

        call_args = str(mock_logger.return_value.info.call_args)
        # Should show seconds, not milliseconds
        # The format is "X.XXs" for seconds
        assert "s" in call_args

    @patch("src.utils.timing.get_run_logger")
    def test_timing_with_exception(self, mock_logger):
        """Test that timing still logs even if exception occurs."""
        mock_logger.return_value = MagicMock()

        with pytest.raises(ValueError):
            with timing("operation with error"):
                time.sleep(0.01)
                raise ValueError("Test error")

        # Should still have logged the timing
        assert mock_logger.return_value.info.called

    @patch("src.utils.timing.get_run_logger")
    def test_timing_elapsed_set_on_exception(self, mock_logger):
        """Test that elapsed_ms is set even when exception occurs."""
        mock_logger.return_value = MagicMock()

        with pytest.raises(ValueError):
            with timing("operation with error") as timing_data:
                time.sleep(0.01)
                raise ValueError("Test error")

        # elapsed_ms should still be set
        assert timing_data["elapsed_ms"] > 0
