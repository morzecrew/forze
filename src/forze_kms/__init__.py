"""KMS backends for Forze envelope encryption (BYOK key management).

A grouping namespace, not an importable adapter: every cloud provider is a
subpackage gated behind its own extra, so importing this package pulls in no
cloud SDK. Import the provider you use directly, e.g.::

    from forze_kms.aws import AwsKmsKeyManagement     # forze[kms-aws]
    from forze_kms.gcp import GcpKmsKeyManagement     # forze[kms-gcp]
    from forze_kms.yc import YcKmsKeyManagement       # forze[kms-yc]
    from forze_kms.local import LocalKeyManagement    # no extra needed

Each backend implements the shared
:class:`~forze.application.contracts.crypto.KeyManagementPort`, so it plugs into
a :class:`~forze.application.execution.CryptoDepsModule` the same way. KMS
providers are grouped here (rather than one top-level ``forze_<provider>kms``
package each) because they are the *same* narrow capability across providers —
unlike distinct capabilities such as object storage vs. queues, which stay in
their own packages. ``local`` is the self-hosted member of the group: it wraps
data keys under operator-provided master keys in process, needs no extra, and
carries its own trust model (see its docstring).
"""
