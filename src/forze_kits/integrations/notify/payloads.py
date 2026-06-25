"""Typed notification commands for worker dispatch."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from forze.base.primitives import JsonDict

# ----------------------- #


class _NotificationCommand(BaseModel):
    """Base for notification commands: frozen at the field level (fields cannot be
    reassigned after construction; nested ``dict`` fields are not deep-frozen)."""

    model_config = ConfigDict(frozen=True)


# ....................... #


class EmailNotification(_NotificationCommand):
    """Send an email via app-provided mailer."""

    kind: Literal["email"] = "email"
    to: str
    subject: str
    body: str
    reply_to: str | None = None


# ....................... #


class PushNotification(_NotificationCommand):
    """Send a mobile or web push notification."""

    kind: Literal["push"] = "push"
    device_token: str
    title: str
    body: str
    data: dict[str, str] = Field(default_factory=dict)


# ....................... #


class WebhookNotification(_NotificationCommand):
    """POST a JSON payload to an HTTPS endpoint."""

    kind: Literal["webhook"] = "webhook"
    url: HttpUrl
    payload: JsonDict = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)


# ....................... #

NotificationCommand = Annotated[
    EmailNotification | PushNotification | WebhookNotification,
    Field(discriminator="kind"),
]
