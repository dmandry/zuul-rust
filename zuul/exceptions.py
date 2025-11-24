# Copyright 2015 Rackspace Australia
# Copyright 2023-2024 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import textwrap

# Error severity
SEVERITY_ERROR = 'error'
SEVERITY_WARNING = 'warning'


class ChangeNotFound(Exception):
    def __init__(self, number, ps):
        self.number = number
        self.ps = ps
        self.change = "%s,%s" % (str(number), str(ps))
        message = "Change %s not found" % self.change
        super(ChangeNotFound, self).__init__(message)


class RevNotFound(Exception):
    def __init__(self, project, rev):
        self.project = project
        self.revision = rev
        message = ("Failed to checkout project '%s' at revision '%s'"
                   % (self.project, self.revision))
        super(RevNotFound, self).__init__(message)


class MergeFailure(Exception):
    pass


class MissingBuildsetError(Exception):
    pass


class ConfigurationError(Exception):
    pass


class StreamingError(Exception):
    pass


class DependencyLimitExceededError(Exception):
    pass


class VariableNameError(Exception):
    pass


# Provider exceptions
class LaunchStatusException(Exception):
    statsd_key = 'error.status'


class LaunchNetworkException(Exception):
    statsd_key = 'error.network'


class LaunchKeyscanException(Exception):
    statsd_key = 'error.keyscan'


class CapacityException(Exception):
    statsd_key = 'error.capacity'


class TimeoutException(Exception):
    pass


class ConnectionTimeoutException(TimeoutException):
    statsd_key = 'error.ssh'


class RuntimeConfigurationException(Exception):
    pass


class QuotaException(Exception):
    pass


class AlgorithmNotSupportedException(Exception):
    pass


# Authentication Exceptions
class AuthTokenException(Exception):
    defaultMsg = 'Unknown Error'
    HTTPError = 400

    def __init__(self, realm=None, msg=None):
        super(AuthTokenException, self).__init__(msg or self.defaultMsg)
        self.realm = realm
        self.error = self.__class__.__name__
        self.error_description = msg or self.defaultMsg

    def getAdditionalHeaders(self):
        return {}


class JWKSException(AuthTokenException):
    defaultMsg = 'Unknown error involving JSON Web Key Set'


class AuthTokenForbiddenException(AuthTokenException):
    defaultMsg = 'Insufficient privileges'
    HTTPError = 403


class AuthTokenUnauthorizedException(AuthTokenException):
    defaultMsg = 'This action requires authentication'
    HTTPError = 401

    def getAdditionalHeaders(self):
        error_header = '''Bearer realm="%s"
       error="%s"
       error_description="%s"'''
        return {"WWW-Authenticate": error_header % (self.realm,
                                                    self.error,
                                                    self.error_description)}


class AuthTokenUndecodedException(AuthTokenUnauthorizedException):
    defaultMsg = 'Auth Token could not be decoded'


class AuthTokenInvalidSignatureException(AuthTokenUnauthorizedException):
    defaultMsg = 'Invalid signature'


class BearerTokenRequiredError(AuthTokenUnauthorizedException):
    defaultMsg = 'Authorization with bearer token required'


class IssuerUnknownError(AuthTokenUnauthorizedException):
    defaultMsg = 'Issuer unknown'


class MissingClaimError(AuthTokenUnauthorizedException):
    defaultMsg = 'Token is missing claims'


class IncorrectAudienceError(AuthTokenUnauthorizedException):
    defaultMsg = 'Incorrect audience'


class TokenExpiredError(AuthTokenUnauthorizedException):
    defaultMsg = 'Token has expired'


class MissingUIDClaimError(MissingClaimError):
    defaultMsg = 'Token is missing id claim'


class IncorrectZuulAdminClaimError(AuthTokenUnauthorizedException):
    defaultMsg = (
        'The "zuul.admin" claim is expected to be a list of tenants')


class UnauthorizedZuulAdminClaimError(AuthTokenUnauthorizedException):
    defaultMsg = 'Issuer is not allowed to set "zuul.admin" claim'


class ConfigurationSyntaxError(Exception):
    zuul_error_name = 'Unknown Configuration Error'
    zuul_error_severity = SEVERITY_ERROR


class NodeFromGroupNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'Node From Group Not Found'

    def __init__(self, nodeset, node, group):
        message = textwrap.dedent("""\
        In {nodeset} the group "{group}" contains a
        node named "{node}" which is not defined in the nodeset.""")
        message = textwrap.fill(message.format(nodeset=nodeset,
                                               node=node, group=group))
        super(NodeFromGroupNotFoundError, self).__init__(message)


class DuplicateNodeError(ConfigurationSyntaxError):
    zuul_error_name = 'Duplicate Node'

    def __init__(self, nodeset, node):
        message = textwrap.dedent("""\
        In nodeset "{nodeset}" the node "{node}" appears multiple times.
        Node names must be unique within a nodeset.""")
        message = textwrap.fill(message.format(nodeset=nodeset,
                                               node=node))
        super(DuplicateNodeError, self).__init__(message)


class UnknownConnection(ConfigurationSyntaxError):
    zuul_error_name = 'Unknown Connection'

    def __init__(self, connection_name):
        message = textwrap.dedent("""\
        Unknown connection named "{connection}".""")
        message = textwrap.fill(message.format(connection=connection_name))
        super(UnknownConnection, self).__init__(message)


class LabelForbiddenError(ConfigurationSyntaxError):
    zuul_error_name = 'Label Forbidden'

    def __init__(self, label, allowed_labels, disallowed_labels):
        message = textwrap.dedent("""\
        Label named "{label}" is not part of the allowed
        labels ({allowed_labels}) for this tenant.""")
        # Make a string that looks like "a, b and not c, d" if we have
        # both allowed and disallowed labels.
        labels = ", ".join(allowed_labels or [])
        if allowed_labels and disallowed_labels:
            labels += ' and '
        if disallowed_labels:
            labels += 'not '
            labels += ", ".join(disallowed_labels)
        message = textwrap.fill(message.format(
            label=label,
            allowed_labels=labels))
        super(LabelForbiddenError, self).__init__(message)


class MaxTimeoutError(ConfigurationSyntaxError):
    zuul_error_name = 'Max Timeout Exceeded'

    def __init__(self, job, tenant):
        message = textwrap.dedent("""\
        The job "{job}" exceeds tenant max-job-timeout {maxtimeout}.""")
        message = textwrap.fill(message.format(
            job=job.name, maxtimeout=tenant.max_job_timeout))
        super(MaxTimeoutError, self).__init__(message)


class PreTimeoutExceedsTimeoutError(ConfigurationSyntaxError):
    zuul_error_name = 'Pre-Timeout Exceeds Timeout'

    def __init__(self, job):
        message = textwrap.dedent("""\
        The job "{job}" has a pre-timeout of {pre_timeout}
        that exceeds its timeout of {timeout}.""")
        message = textwrap.fill(message.format(
            job=job.name, pre_timeout=job.pre_timeout, timeout=job.timeout))
        super(PreTimeoutExceedsTimeoutError, self).__init__(message)


class MaxOIDCTTLError(ConfigurationSyntaxError):
    zuul_error_name = 'Max OIDC TTL Exceeded'

    def __init__(self, secret, tenant):
        message = textwrap.dedent("""\
        The oidc secret "{secret}" exceeds tenant max-oidc-ttl {max_ttl}.""")
        message = textwrap.fill(message.format(
            secret=secret.name, max_ttl=tenant.max_oidc_ttl))
        super(MaxOIDCTTLError, self).__init__(message)


class OIDCIssuerNotAllowedError(ConfigurationSyntaxError):
    zuul_error_name = 'OIDC issuer not allowed'

    def __init__(self, secret, issuer):
        message = textwrap.dedent("""\
        The iss "{issuer}" in oidc secret "{secret}" is not allowed.""")
        message = textwrap.fill(message.format(
            secret=secret.name, issuer=issuer))
        super(OIDCIssuerNotAllowedError, self).__init__(message)


class DuplicateGroupError(ConfigurationSyntaxError):
    zuul_error_name = 'Duplicate Nodeset Group'

    def __init__(self, nodeset, group):
        message = textwrap.dedent("""\
        In {nodeset} the group "{group}" appears multiple times.
        Group names must be unique within a nodeset.""")
        message = textwrap.fill(message.format(nodeset=nodeset,
                                               group=group))
        super(DuplicateGroupError, self).__init__(message)


class ReporterJobNotPermittedError(ConfigurationSyntaxError):
    zuul_error_name = 'Reporter Job Not Permitted'

    def __init__(self, project):
        message = textwrap.dedent(f"""\
        The project {project} is not permitted to configure a
        reporter job.""")
        message = textwrap.fill(message)
        super().__init__(message)


class ProjectNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'Project Not Found'

    def __init__(self, project):
        projects = None
        if isinstance(project, (list, tuple)):
            if len(project) > 1:
                projects = ', '.join(f'"{p}"' for p in project)
            else:
                project = project[0]
        if projects:
            message = textwrap.dedent(f"""\
            The projects {projects} were not found.  All projects
            referenced within a Zuul configuration must first be
            added to the main configuration file by the Zuul
            administrator.""")
        else:
            message = textwrap.dedent(f"""\
            The project "{project}" was not found.  All projects
            referenced within a Zuul configuration must first be
            added to the main configuration file by the Zuul
            administrator.""")
        message = textwrap.fill(message)
        super(ProjectNotFoundError, self).__init__(message)


class TemplateNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'Template Not Found'

    def __init__(self, template):
        message = textwrap.dedent("""\
        The project template "{template}" was not found.
        """)
        message = textwrap.fill(message.format(template=template))
        super(TemplateNotFoundError, self).__init__(message)


class NodesetNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'Nodeset Not Found'

    def __init__(self, nodeset):
        message = textwrap.dedent("""\
        The nodeset "{nodeset}" was not found.
        """)
        message = textwrap.fill(message.format(nodeset=nodeset))
        super(NodesetNotFoundError, self).__init__(message)


class LabelNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'Label Not Found'

    def __init__(self, label):
        message = textwrap.dedent("""\
        The label "{label}" was not found.
        """)
        message = textwrap.fill(message.format(label=label))
        super(LabelNotFoundError, self).__init__(message)


class PipelineNotPermittedError(ConfigurationSyntaxError):
    zuul_error_name = 'Pipeline Forbidden'

    def __init__(self):
        message = textwrap.dedent("""\
        Pipelines may not be defined in untrusted repos,
        they may only be defined in config repos.""")
        message = textwrap.fill(message)
        super(PipelineNotPermittedError, self).__init__(message)


class ProjectNotPermittedError(ConfigurationSyntaxError):
    zuul_error_name = 'Project Forbidden'

    def __init__(self):
        message = textwrap.dedent("""\
        Within an untrusted project, the only project definition
        permitted is that of the project itself.""")
        message = textwrap.fill(message)
        super(ProjectNotPermittedError, self).__init__(message)


class AuthZRuleNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'AuthZ Rule Not Found'

    def __init__(self, authz_rule):
        message = textwrap.dedent("""\
        The Authorization Rule "{authz_rule}" was not found.  All
        RBAC role mappings must reference valid AuthZ Rules.""")
        message = textwrap.fill(message.format(authz_rule=authz_rule))
        super(AuthZRuleNotFoundError, self).__init__(message)


class AuthZRoleNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'AuthZ Role Not Found'

    def __init__(self, authz_role):
        message = textwrap.dedent("""\
        The Authorization Role "{authz_role}" was not found.  All
        RBAC role mappings must reference valid AuthZ Roles.""")
        message = textwrap.fill(message.format(authz_role=authz_role))
        super(AuthZRoleNotFoundError, self).__init__(message)


class GlobalSemaphoreNotFoundError(ConfigurationSyntaxError):
    zuul_error_name = 'Global Semaphore Not Found'

    def __init__(self, semaphore):
        message = textwrap.dedent("""\
        The global semaphore "{semaphore}" was not found.  All
        global semaphores must be added to the main configuration
        file by the Zuul administrator.""")
        message = textwrap.fill(message.format(semaphore=semaphore))
        super(GlobalSemaphoreNotFoundError, self).__init__(message)


class YAMLDuplicateKeyError(ConfigurationSyntaxError):
    def __init__(self, key, source_context, start_mark):
        self.source_context = source_context
        self.start_mark = start_mark
        message = (f'The key "{key}" appears more than once; '
                   'duplicate keys are not permitted.')
        super(YAMLDuplicateKeyError, self).__init__(message)


class ConfigurationSyntaxWarning:
    zuul_error_name = 'Unknown Configuration Warning'
    zuul_error_severity = SEVERITY_WARNING
    zuul_error_message = 'Unknown Configuration Warning'

    def __init__(self, message=None):
        if message:
            self.zuul_error_message = message


class MultipleProjectConfigurations(ConfigurationSyntaxWarning):
    zuul_error_name = 'Multiple Project Configurations'
    zuul_error_problem = 'configuration error'

    def __init__(self, source_context):
        message = textwrap.dedent(f"""\
        Configuration in {source_context.path} ignored because project-branch
        is already configured.""")
        message = textwrap.fill(message)
        super().__init__(message)


class DeprecationWarning(ConfigurationSyntaxWarning):
    zuul_error_problem = 'deprecated syntax'


class RegexDeprecation(DeprecationWarning):
    zuul_error_name = 'Regex Deprecation'
    zuul_error_message = """\
All regular expressions must conform to RE2 syntax, but an
expression using the deprecated Perl-style syntax has been detected.
Adjust the configuration to conform to RE2 syntax."""

    def __init__(self, message=None):
        if message:
            message = (self.zuul_error_message +
                       f"\n\nThe RE2 syntax error is: {message}")
        super().__init__(message)


class CleanupRunDeprecation(DeprecationWarning):
    zuul_error_name = 'Cleanup Run Deprecation'
    zuul_error_message = """\
The cleanup-run job attribute is deprecated.  Replace it with
post-run playbooks with the `cleanup` attribute set."""
