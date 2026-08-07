from pathlib import Path

import pytest

from meeting_action_orchestrator.application.errors import (
    DeliveryGatewayError,
    PermanentDeliveryError,
    ProviderConfigurationError,
    ProviderInputError,
    ProviderOutputError,
    ProviderTransientError,
    RetryableDeliveryError,
    UnknownDeliveryOutcomeError,
)
from meeting_action_orchestrator.application.ports import (
    AudioMetadata as ApplicationAudioMetadata,
)
from meeting_action_orchestrator.application.ports import StoredAudio as ApplicationStoredAudio
from meeting_action_orchestrator.infrastructure.audio import AudioMetadata, StoredAudio
from meeting_action_orchestrator.infrastructure.mcp_gateway import (
    McpGatewayError,
    PermanentMcpError,
    RetryableMcpError,
    UnknownMcpOutcomeError,
)
from meeting_action_orchestrator.infrastructure.openai_agents import (
    OpenAIAgentConfigurationError,
    OpenAIAgentOutputError,
    OpenAIAgentTransientError,
)
from meeting_action_orchestrator.infrastructure.openai_transcription import (
    OpenAITranscriptionConfigurationError,
    OpenAITranscriptionInputError,
    OpenAITranscriptionOutputError,
    OpenAITranscriptionTransientError,
)

APPLICATION_ROOT = Path(__file__).parents[2] / "src" / "meeting_action_orchestrator" / "application"


def test_application_layer_does_not_import_infrastructure() -> None:
    violations = {
        path.relative_to(APPLICATION_ROOT)
        for path in APPLICATION_ROOT.glob("*.py")
        if "meeting_action_orchestrator.infrastructure" in path.read_text(encoding="utf-8")
    }

    assert violations == set()


def test_audio_contracts_remain_available_from_the_infrastructure_module() -> None:
    assert AudioMetadata is ApplicationAudioMetadata
    assert StoredAudio is ApplicationStoredAudio


@pytest.mark.parametrize(
    ("implementation", "contract"),
    [
        (OpenAIAgentConfigurationError, ProviderConfigurationError),
        (OpenAIAgentTransientError, ProviderTransientError),
        (OpenAIAgentOutputError, ProviderOutputError),
        (OpenAITranscriptionConfigurationError, ProviderConfigurationError),
        (OpenAITranscriptionInputError, ProviderInputError),
        (OpenAITranscriptionTransientError, ProviderTransientError),
        (OpenAITranscriptionOutputError, ProviderOutputError),
    ],
)
def test_openai_errors_implement_application_provider_contracts(
    implementation: type[Exception],
    contract: type[Exception],
) -> None:
    assert issubclass(implementation, contract)


@pytest.mark.parametrize(
    ("implementation", "contract"),
    [
        (RetryableMcpError, RetryableDeliveryError),
        (PermanentMcpError, PermanentDeliveryError),
        (UnknownMcpOutcomeError, UnknownDeliveryOutcomeError),
    ],
)
def test_mcp_errors_implement_application_delivery_contracts(
    implementation: type[Exception],
    contract: type[Exception],
) -> None:
    assert issubclass(implementation, contract)
    assert issubclass(implementation, McpGatewayError)
    assert issubclass(implementation, DeliveryGatewayError)
