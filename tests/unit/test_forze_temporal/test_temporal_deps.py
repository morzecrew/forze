import pytest

from forze_temporal.execution.deps import (
    ConfigurableTemporalWorkflowCommand,
    ConfigurableTemporalWorkflowQuery,
    ConfigurableTemporalWorkflowScheduleCommand,
    ConfigurableTemporalWorkflowScheduleQuery,
    TemporalWorkflowConfig,
)


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="TemporalWorkflowConfig"):
        ConfigurableTemporalWorkflowQuery(config={"queue": "q"})

    with pytest.raises(TypeError, match="TemporalWorkflowConfig"):
        ConfigurableTemporalWorkflowCommand(config={"queue": "q"})

    with pytest.raises(TypeError, match="TemporalWorkflowConfig"):
        ConfigurableTemporalWorkflowScheduleQuery(config={"queue": "q"})

    with pytest.raises(TypeError, match="TemporalWorkflowConfig"):
        ConfigurableTemporalWorkflowScheduleCommand(config={"queue": "q"})
