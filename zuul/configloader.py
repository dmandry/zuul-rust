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

import collections
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import logging
import math
import os
import re
import re2
import subprocess
import textwrap
import threading
import types

import voluptuous as vs

from zuul import change_matcher
from zuul import model
from zuul.connection import ReadOnlyBranchCacheError
from zuul.lib import yamlutil as yaml
from zuul.lib import varnames
import zuul.manager.dependent
import zuul.manager.independent
import zuul.manager.supercedent
import zuul.manager.serial
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.re2util import ZuulRegex
from zuul.lib.varnames import check_varnames
from zuul.zk.semaphore import SemaphoreHandler
from zuul.exceptions import (
    AlgorithmNotSupportedException,
    AuthZRuleNotFoundError,
    AuthZRoleNotFoundError,
    CleanupRunDeprecation,
    DuplicateGroupError,
    DuplicateNodeError,
    GlobalSemaphoreNotFoundError,
    MultipleProjectConfigurations,
    NodeFromGroupNotFoundError,
    PipelineNotPermittedError,
    ProjectNotFoundError,
    RegexDeprecation,
    SEVERITY_ERROR,
    TemplateNotFoundError,
    UnknownConnection,
    YAMLDuplicateKeyError,
)

ZUUL_CONF_ROOT = ('zuul.yaml', 'zuul.d', '.zuul.yaml', '.zuul.d')

# A voluptuous schema for a regular expression.
ZUUL_REGEX = {
    vs.Required('regex'): str,
    'negate': bool,
}


# Several forms accept either a single item or a list, this makes
# specifying that in the schema easy (and explicit).
def to_list(x):
    return vs.Any([x], x)


def override_value(x):
    schema = vs.Schema(x)

    def validator(v):
        if isinstance(v, yaml.OverrideValue):
            v = v.value
        schema(v)
    return validator


def override_list(x):
    return override_value(to_list(x))


def as_list(item):
    if not item:
        return []
    if isinstance(item, list):
        return item
    return [item]


# Allow "- foo" as a shorthand for "- {name: foo}"
def dict_with_name(item):
    if isinstance(item, str):
        return {'name': item}
    return item


def no_dup_config_paths(v):
    if isinstance(v, list):
        for x in v:
            check_config_path(x)
    elif isinstance(v, str):
        check_config_path(v)
    else:
        raise vs.Invalid("Expected str or list of str for extra-config-paths")


def check_config_path(path):
    if not isinstance(path, str):
        raise vs.Invalid("Expected str or list of str for extra-config-paths")
    elif path in ["zuul.yaml", "zuul.d/", ".zuul.yaml", ".zuul.d/"]:
        raise vs.Invalid("Default zuul configs are not "
                         "allowed in extra-config-paths")


def make_regex(data, parse_context=None):
    if isinstance(data, dict):
        regex = ZuulRegex(data['regex'],
                          negate=data.get('negate', False))
    else:
        regex = ZuulRegex(data)
    if parse_context and regex.re2_failure:
        if regex.re2_failure_message:
            parse_context.accumulator.addError(RegexDeprecation(
                regex.re2_failure_message))
        else:
            parse_context.accumulator.addError(RegexDeprecation())
    return regex


def indent(s):
    return '\n'.join(['  ' + x for x in s.split('\n')])


class LocalAccumulator:
    """An error accumulator that holds local context information."""
    def __init__(self, error_list, source_context=None, stanza=None,
                 conf=None, attr=None):
        self.error_list = error_list
        self.source_context = source_context
        self.stanza = stanza
        self.conf = conf
        self.attr = attr

    def extend(self, error_list=None, source_context=None, stanza=None,
               conf=None, attr=None):
        """Return a new accumulator that extends this one with additional
        info"""
        if conf:
            if isinstance(conf, (types.MappingProxyType, dict)):
                conf_context = conf.get('_source_context')
            else:
                conf_context = getattr(conf, 'source_context', None)
            source_context = source_context or conf_context
        if error_list is None:
            error_list = self.error_list
        return LocalAccumulator(error_list,
                                source_context or self.source_context,
                                stanza or self.stanza,
                                conf or self.conf,
                                attr or self.attr)

    @contextmanager
    def catchErrors(self):
        try:
            yield
        except ReadOnlyBranchCacheError:
            raise
        except Exception as exc:
            self.addError(exc)

    def addError(self, error):
        """Adds the error or warning to the accumulator.

        The input can be any exception or any object with the
        zuul_error attributes.

        If the error has a source_context or start_mark attribute,
        this method will use those.

        This method will produce the most detailed error message it
        can with the data supplied by the error and the most recent
        error context.
        """

        # A list of paragraphs
        msg = []

        repo = branch = None

        source_context = getattr(error, 'source_context', self.source_context)
        if source_context:
            repo = source_context.project_name
            branch = source_context.branch
        stanza = self.stanza

        problem = getattr(error, 'zuul_error_problem', 'syntax error')
        if problem[0] in 'aoeui':
            a_an = 'an'
        else:
            a_an = 'a'

        if repo and branch:
            intro = f"""\
            Zuul encountered {a_an} {problem} while parsing its
            configuration in the repo {repo} on branch {branch}.  The
            problem was:"""
        elif repo:
            intro = f"""\
            Zuul encountered an error while accessing the repo {repo}.
            The error was:"""
        else:
            intro = "Zuul encountered an error:"

        msg.append(textwrap.dedent(intro))

        error_text = getattr(error, 'zuul_error_message', str(error))
        msg.append(indent(error_text))

        snippet = start_mark = name = line = location = None
        attr = self.attr
        if self.conf:
            if isinstance(self.conf, (types.MappingProxyType, dict)):
                name = self.conf.get('name')
                start_mark = self.conf.get('_start_mark')
            else:
                name = getattr(self.conf, 'name', None)
                start_mark = getattr(self.conf, 'start_mark', None)
        if start_mark is None:
            start_mark = getattr(error, 'start_mark', None)
        if start_mark:
            line = start_mark.line
            if attr is not None:
                line = getattr(attr, 'line', line)
            snippet = start_mark.getLineSnippet(line).rstrip()
            location = start_mark.getLineLocation(line)
        if name:
            name = f'"{name}"'
        else:
            name = 'following'
        if stanza:
            if attr is not None:
                pointer = (
                    f'The problem appears in the "{attr}" attribute\n'
                    f'of the {name} {stanza} stanza:'
                )
            else:
                pointer = (
                    f'The problem appears in the {name} {stanza} stanza:'
                )
            msg.append(pointer)

        if snippet:
            msg.append(indent(snippet))
        if location:
            msg.append(location)

        error_message = '\n\n'.join(msg)
        error_severity = getattr(error, 'zuul_error_severity',
                                 SEVERITY_ERROR)
        error_name = getattr(error, 'zuul_error_name', 'Unknown')

        config_error = model.ConfigurationError(
            source_context, start_mark, error_message,
            short_error=error_text,
            severity=error_severity,
            name=error_name)
        self.error_list.append(config_error)


class ZuulSafeLoader(yaml.EncryptedLoader):
    zuul_node_types = frozenset(('job', 'nodeset', 'secret', 'pipeline',
                                 'project', 'project-template',
                                 'semaphore', 'queue', 'pragma',
                                 'image', 'flavor', 'label', 'section',
                                 'provider'))

    def __init__(self, stream, source_context):
        wrapped_stream = io.StringIO(stream)
        wrapped_stream.name = str(source_context)
        super(ZuulSafeLoader, self).__init__(wrapped_stream)
        self.name = str(source_context)
        self.zuul_context = source_context
        self.zuul_stream = stream

    def construct_mapping(self, node, deep=False):
        keys = set()
        for k, v in node.value:
            # The key << needs to be treated special since that will merge
            # the anchor into the mapping and not create a key on its own.
            if k.value == '<<':
                continue

            if not isinstance(k.value, collections.abc.Hashable):
                # This happens with "foo: {{ bar }}"
                # This will raise an error in the superclass
                # construct_mapping below; ignore it for now.
                continue

            if k.value in keys:
                mark = model.ZuulMark(node.start_mark, node.end_mark,
                                      self.zuul_stream)
                raise YAMLDuplicateKeyError(k.value, self.zuul_context,
                                            mark)
            keys.add(k.value)
            if k.tag == 'tag:yaml.org,2002:str':
                k.value = yaml.ZuulConfigKey(k.value, node.start_mark.line)
        r = super(ZuulSafeLoader, self).construct_mapping(node, deep)
        keys = frozenset(r.keys())
        if len(keys) == 1 and keys.intersection(self.zuul_node_types):
            d = list(r.values())[0]
            if isinstance(d, dict):
                d['_start_mark'] = model.ZuulMark(node.start_mark,
                                                  node.end_mark,
                                                  self.zuul_stream)
                d['_source_context'] = self.zuul_context
        return r


def safe_load_yaml(stream, source_context):
    loader = ZuulSafeLoader(stream, source_context)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


def ansible_var_name(value):
    vs.Schema(str)(value)
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", value):
        raise vs.Invalid("Invalid Ansible variable name '{}'".format(value))


def ansible_vars_dict(value):
    vs.Schema(dict)(value)
    for key in value:
        ansible_var_name(key)


class PragmaParser(object):
    pragma = {
        'implied-branch-matchers': bool,
        'implied-branches': to_list(vs.Any(ZUUL_REGEX, str)),
        '_source_context': model.SourceContext,
        '_start_mark': model.ZuulMark,
    }

    schema = vs.Schema(pragma)

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.PragmaParser")
        self.pcontext = pcontext

    def fromYaml(self, conf):
        self.schema(conf)
        bm = conf.get('implied-branch-matchers')

        source_context = conf['_source_context']
        if bm is not None:
            source_context.implied_branch_matchers = bm

        with self.pcontext.confAttr(conf, 'implied-branches') as branches:
            if branches is not None:
                # This is a BranchMatcher (not an ImpliedBranchMatcher)
                # because as user input, we allow/expect this to be
                # regular expressions.  Only truly implicit branch names
                # (automatically generated from source file branches) are
                # ImpliedBranchMatchers.
                source_context.implied_branches = [
                    change_matcher.BranchMatcher(make_regex(x, self.pcontext))
                    for x in as_list(branches)]
                source_context.implied_branch_matcher = \
                    change_matcher.MatchAny(source_context.implied_branches)

        if source_context.implied_branch_matcher is None:
            source_context.implied_branch_matcher =\
                change_matcher.ImpliedBranchMatcher(
                    ZuulRegex(source_context.branch))


class ImageParser(object):
    image = {
        '_source_context': model.SourceContext,
        '_start_mark': model.ZuulMark,
        vs.Required('name'): str,
        vs.Required('type'): vs.Any('zuul', 'cloud'),
        'description': str,
        'retain-count': vs.All(int, vs.Range(min=1)),
        'max-retain-count': vs.All(int, vs.Range(min=1)),
    }
    schema = vs.Schema(image)

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.ImageParser")
        self.pcontext = pcontext

    def fromYaml(self, conf):
        self.schema(conf)

        image = model.Image(conf['name'], conf['type'],
                            conf.get('description'))
        image.source_context = conf.get('_source_context')
        image.start_mark = conf.get('_start_mark')
        return image


class FlavorParser(object):
    flavor = {
        '_source_context': model.SourceContext,
        '_start_mark': model.ZuulMark,
        vs.Required('name'): str,
        'description': str,
    }
    schema = vs.Schema(flavor)

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.FlavorParser")
        self.pcontext = pcontext

    def fromYaml(self, conf):
        self.schema(conf)

        flavor = model.Flavor(conf['name'], conf.get('description'))
        flavor.source_context = conf.get('_source_context')
        flavor.start_mark = conf.get('_start_mark')
        return flavor


class LabelParser(object):
    label = {
        '_source_context': model.SourceContext,
        '_start_mark': model.ZuulMark,
        vs.Required('name'): str,
        vs.Required('image'): str,
        vs.Required('flavor'): str,
        'description': str,
        'min-ready': int,
        'max-ready-age': int,
        'max-age': int,
        'min-retention-time': int,
        'max-nodes': int,
    }
    schema = vs.Schema(label)

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.LabelParser")
        self.pcontext = pcontext

    def fromYaml(self, conf):
        self.schema(conf)

        label = model.Label(conf['name'], conf['image'], conf['flavor'],
                            conf.get('description'), conf.get('min-ready'),
                            conf.get('max-ready-age'), conf.get('max-age'),
                            conf.get('max-nodes'),
                            conf.get('min-retention-time'))
        label.source_context = conf.get('_source_context')
        label.start_mark = conf.get('_start_mark')
        return label


class SectionParser(object):
    section = {
        '_source_context': model.SourceContext,
        '_start_mark': model.ZuulMark,
        vs.Required('name'): str,
        'parent': str,
        'abstract': bool,
        'connection': str,
        'description': str,
        'images': [vs.Any(str, dict)],
        'flavors': [vs.Any(str, dict)],
        'labels': [vs.Any(str, dict)],
    }
    schema = vs.Schema(section, extra=vs.ALLOW_EXTRA)

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.SectionParser")
        self.pcontext = pcontext

    def fromYaml(self, conf):
        self.schema(conf)

        # Mutate these into the expected form
        conf['images'] = [dict_with_name(x) for x in conf.get('images', [])]
        conf['flavors'] = [dict_with_name(x) for x in conf.get('flavors', [])]
        conf['labels'] = [dict_with_name(x) for x in conf.get('labels', [])]

        section = model.Section(conf['name'])
        section.parent = conf.get('parent')
        section.abstract = conf.get('abstract')
        section.connection = conf.get('connection')
        section.description = conf.get('description')
        section.source_context = conf.get('_source_context')
        section.start_mark = conf.get('_start_mark')
        section.config = conf
        return section


class ProviderParser(object):
    provider = {
        '_source_context': model.SourceContext,
        '_start_mark': model.ZuulMark,
        vs.Required('name'): str,
        vs.Required('section'): str,
        'description': str,
        'images': [vs.Any(str, dict)],
        'labels': [vs.Any(str, dict)],
    }
    schema = vs.Schema(provider, extra=vs.ALLOW_EXTRA)

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.ProviderParser")
        self.pcontext = pcontext

    def fromYaml(self, conf):
        self.schema(conf)

        if 'flavors' in conf:
            raise Exception("Flavor configuration not permitted in Provider, "
                            "only Section")

        # Mutate these into the expected form
        conf['images'] = [dict_with_name(x) for x in conf.get('images', [])]
        conf['labels'] = [dict_with_name(x) for x in conf.get('labels', [])]

        provider_config = model.ProviderConfig(conf['name'],
                                               conf['section'])
        provider_config.description = conf.get('description')
        provider_config.source_context = conf.get('_source_context')
        provider_config.start_mark = conf.get('_start_mark')
        provider_config.config = conf
        return provider_config


class NodeSetParser(object):
    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.NodeSetParser")
        self.pcontext = pcontext
        self.anonymous = False
        self.schema = self.getSchema(False)
        self.anon_schema = self.getSchema(True)

    def getSchema(self, anonymous=False):
        node = {vs.Required('name'): to_list(str),
                vs.Required('label'): str,
                }

        group = {vs.Required('name'): str,
                 vs.Required('nodes'): to_list(str),
                 }

        real_nodeset = {vs.Required('nodes'): to_list(node),
                        'groups': to_list(group),
                        }

        alt_nodeset = {vs.Required('alternatives'):
                       [vs.Any(real_nodeset, str)]}

        top_nodeset = {'_source_context': model.SourceContext,
                       '_start_mark': model.ZuulMark,
                       }
        if not anonymous:
            top_nodeset[vs.Required('name')] = str

        top_real_nodeset = real_nodeset.copy()
        top_real_nodeset.update(top_nodeset)
        top_alt_nodeset = alt_nodeset.copy()
        top_alt_nodeset.update(top_nodeset)

        nodeset = vs.Any(top_real_nodeset, top_alt_nodeset)

        return vs.Schema(nodeset)

    def fromYaml(self, conf, anonymous=False):
        if anonymous:
            self.anon_schema(conf)
            self.anonymous = True
        else:
            self.schema(conf)

        if 'alternatives' in conf:
            return self.loadAlternatives(conf)
        else:
            return self.loadNodeset(conf)

    def loadAlternatives(self, conf):
        ns = model.NodeSet(conf.get('name'))
        ns.source_context = conf.get('_source_context')
        ns.start_mark = conf.get('_start_mark')
        for alt in conf['alternatives']:
            if isinstance(alt, str):
                ns.addAlternative(alt)
            else:
                ns.addAlternative(self.loadNodeset(alt))
        return ns

    def loadNodeset(self, conf):
        ns = model.NodeSet(conf.get('name'))
        ns.source_context = conf.get('_source_context')
        ns.start_mark = conf.get('_start_mark')
        node_names = set()
        group_names = set()

        for conf_node in as_list(conf['nodes']):
            if "localhost" in as_list(conf_node['name']):
                raise Exception("Nodes named 'localhost' are not allowed.")
            for name in as_list(conf_node['name']):
                if name in node_names:
                    raise DuplicateNodeError(name, conf_node['name'])
            node = model.Node(as_list(conf_node['name']), conf_node['label'])
            ns.addNode(node)
            for name in as_list(conf_node['name']):
                node_names.add(name)
        for conf_group in as_list(conf.get('groups', [])):
            if "localhost" in conf_group['name']:
                raise Exception("Groups named 'localhost' are not allowed.")
            for node_name in as_list(conf_group['nodes']):
                if node_name not in node_names:
                    nodeset_str = 'the nodeset' if self.anonymous else \
                        'the nodeset "%s"' % conf['name']
                    raise NodeFromGroupNotFoundError(nodeset_str, node_name,
                                                     conf_group['name'])
            if conf_group['name'] in group_names:
                nodeset_str = 'the nodeset' if self.anonymous else \
                    'the nodeset "%s"' % conf['name']
                raise DuplicateGroupError(nodeset_str, conf_group['name'])
            group = model.Group(conf_group['name'],
                                as_list(conf_group['nodes']))
            ns.addGroup(group)
            group_names.add(conf_group['name'])
        return ns


class SecretParser(object):
    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.SecretParser")
        self.pcontext = pcontext
        self.schema = self.getSchema()

    def _checkMissingAttribute(self, secret):
        if 'data' not in secret and 'oidc' not in secret:
            raise vs.Invalid("Either 'data' or 'oidc' must be present.")
        return secret

    def getSchema(self):
        oidc_schema = vs.Any({
            'algorithm': str,
            'ttl': int,
            'iss': str,
            'claims': dict,
        }, None)

        secret = vs.All({
            vs.Required('name'): str,
            vs.Exclusive('data', 'secret_type'): dict,
            vs.Exclusive('oidc', 'secret_type'): oidc_schema,
            '_source_context': model.SourceContext,
            '_start_mark': model.ZuulMark,
        }, self._checkMissingAttribute)

        return vs.Schema(secret)

    def fromYaml(self, conf):
        self.schema(conf)
        s = model.Secret(conf['name'], conf['_source_context'])
        s.source_context = conf['_source_context']
        s.start_mark = conf['_start_mark']
        if 'data' in conf:
            s.secret_data = conf['data']
        else:
            # TODO: A change to the global config would require
            # reloading the file where the secret is defined; this may
            # not be obvious to users, but it's also probably not
            # something we need to immediately solve.  It probably
            # would be fixed by a full reconfiguration.
            glbl = self.pcontext.globals
            if conf['oidc'] is None:
                conf['oidc'] = {}
            algorithm = conf['oidc'].get('algorithm')
            if not algorithm:
                conf['oidc']['algorithm'] = glbl.oidc_default_signing_algorithm
            elif algorithm not in glbl.oidc_supported_signing_algorithms:
                raise AlgorithmNotSupportedException(
                    f"Algorithm '{algorithm}' is not supported")

            s.secret_oidc = conf['oidc']
        return s


class JobParser(object):
    ANSIBLE_ROLE_RE = re.compile(r'^(ansible[-_.+]*)*(role[-_.+]*)*')

    zuul_role = {vs.Required('zuul'): str,
                 'name': str}

    galaxy_role = {vs.Required('galaxy'): str,
                   'name': str}

    role = vs.Any(zuul_role, galaxy_role)

    job_project = {vs.Required('name'): str,
                   'override-branch': str,
                   'override-checkout': str}

    include_project_name = {'type': 'name', 'name': str}
    include_project_change = {'type': 'change'}
    include_project_item = {'type': 'item'}
    job_include_project = vs.Any(
        str,
        include_project_name,
        include_project_change,
        include_project_item,
    )

    job_dependency = {vs.Required('name'): str,
                      'soft': bool}

    secret = {vs.Required('name'): ansible_var_name,
              vs.Required('secret'): str,
              'pass-to-parent': bool}

    semaphore = {vs.Required('name'): str,
                 'resources-first': bool}

    complex_playbook_def = {
        vs.Required('name'): str,
        'semaphores': to_list(str),
    }

    playbook_def = to_list(vs.Any(str, complex_playbook_def))

    post_run_playbook_def = {
        vs.Required('name'): str,
        'semaphores': to_list(str),
        'cleanup': bool,
    }

    post_run_playbook_def = to_list(vs.Any(str, post_run_playbook_def))

    complex_include_vars_project_def = {
        vs.Required('name'): str,
        'project': str,
        'required': bool,
    }

    complex_include_vars_zuul_project_def = {
        vs.Required('name'): str,
        'zuul-project': bool,
        'use-ref': bool,
        'required': bool,
    }

    include_vars_def = to_list(vs.Any(str,
                                      complex_include_vars_project_def,
                                      complex_include_vars_zuul_project_def,
                                      ))

    # Attributes of a job that can also be used in Project and ProjectTemplate
    job_attributes = {'parent': vs.Any(str, None),
                      'final': bool,
                      'abstract': bool,
                      'protected': bool,
                      'intermediate': bool,
                      'requires': override_list(str),
                      'provides': override_list(str),
                      'failure-message': str,
                      'success-message': str,
                      # TODO: ignored, remove for v5
                      'failure-url': str,
                      # TODO: ignored, remove for v5
                      'success-url': str,
                      'hold-following-changes': bool,
                      'voting': bool,
                      'semaphore': vs.Any(semaphore, str),
                      'semaphores': to_list(vs.Any(semaphore, str)),
                      'tags': override_list(str),
                      'branches': to_list(vs.Any(ZUUL_REGEX, str)),
                      'files': override_list(vs.Any(ZUUL_REGEX, str)),
                      'secrets': to_list(vs.Any(secret, str)),
                      'irrelevant-files': override_list(
                          vs.Any(ZUUL_REGEX, str)),
                      # validation happens in NodeSetParser
                      'nodeset': vs.Any(dict, str),
                      'pre-timeout': int,
                      'timeout': int,
                      'post-timeout': int,
                      'attempts': int,
                      'pre-run': playbook_def,
                      'post-run': post_run_playbook_def,
                      'run': playbook_def,
                      'cleanup-run': playbook_def,
                      'ansible-split-streams': bool,
                      'ansible-version': vs.Any(str, float, int),
                      '_source_context': model.SourceContext,
                      '_start_mark': model.ZuulMark,
                      'roles': to_list(role),
                      'required-projects': override_list(
                          vs.Any(job_project, str)),
                      'include-projects': override_list(job_include_project),
                      'exclude-projects': override_list(job_include_project),
                      'vars': override_value(ansible_vars_dict),
                      'extra-vars': override_value(ansible_vars_dict),
                      'host-vars': override_value({str: ansible_vars_dict}),
                      'group-vars': override_value({str: ansible_vars_dict}),
                      'include-vars': override_value(include_vars_def),
                      'dependencies': override_list(
                          vs.Any(job_dependency, str)),
                      'allowed-projects': to_list(str),
                      'override-branch': str,
                      'override-checkout': str,
                      'description': str,
                      'variant-description': str,
                      'post-review': bool,
                      'match-on-config-updates': bool,
                      'workspace-scheme': vs.Any('golang', 'flat', 'unique'),
                      'workspace-checkout': vs.Any(bool, 'auto'),
                      'deduplicate': vs.Any(bool, 'auto'),
                      'failure-output': override_list(str),
                      'image-build-name': str,
                      'type': vs.Any('regular', 'initializer', 'reporter'),
                      'attribute-control': {
                          vs.Any(
                              'requires',
                              'provides',
                              'tags',
                              'files',
                              'irrelevant-files',
                              'required-projects',
                              'include-projects',
                              'exclude-projects',
                              'vars',
                              'extra-vars',
                              'host-vars',
                              'group-vars',
                              'include-vars',
                              'dependencies',
                              'failure-output',
                              'type',
                              'pre-run',
                              'run',
                              'post-run',
                              # Normally we don't include context
                              # attributes since they are not affected
                              # by "final", but we make an exception
                              # here for "voting" so that users can
                              # specify that a job must never be
                              # non-voting.
                              'voting',
                          ): {'final': True},
    }}

    job_name = {vs.Required('name'): str}

    job = dict(collections.ChainMap(job_name, job_attributes))

    schema = vs.Schema(job)

    simple_attributes = [
        'final',
        'abstract',
        'protected',
        'intermediate',
        'pre-timeout',
        'timeout',
        'post-timeout',
        'workspace',
        'voting',
        'hold-following-changes',
        'attempts',
        'failure-message',
        'success-message',
        'override-branch',
        'override-checkout',
        'match-on-config-updates',
        'workspace-scheme',
        'workspace-checkout',
        'deduplicate',
        'image-build-name',
        'type',
    ]

    attr_control_job_attr_map = {
        'failure-message': 'failure_message',
        'success-message': 'success_message',
        'failure-url': 'failure_url',
        'success-url': 'success_url',
        'hold-following-changes': 'hold_following_changes',
        'files': 'file_matcher',
        'irrelevant-files': 'irrelevant_file_matcher',
        'pre-timeout': 'pre_timeout',
        'post-timeout': 'post_timeout',
        'pre-run': 'pre_run',
        'post-run': 'post_run',
        'cleanup-run': 'cleanup_run',
        'ansible-split-streams': 'ansible_split_streams',
        'ansible-version': 'ansible_version',
        'required-projects': 'required_projects',
        'include-projects': 'include_projects',
        'exclude-projects': 'exclude_projects',
        'vars': 'variables',
        'extra-vars': 'extra_variables',
        'host-vars': 'host_variables',
        'group-vars': 'group_variables',
        'include-vars': 'include_vars',
        'allowed-projects': 'allowed_projects',
        'override-branch': 'override_branch',
        'override-checkout': 'override_checkout',
        'variant-description': 'variant_description',
        'workspace-scheme': 'workspace_scheme',
        'failure-output': 'failure_output',
        'image-build-name': 'image_build_name',
    }

    def _getAttrControlJobAttr(self, attr):
        return self.attr_control_job_attr_map.get(attr, attr)

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.JobParser")
        self.pcontext = pcontext

    @staticmethod
    def _makeVariantDescription(conf):
        parts = []
        for b in as_list(conf.get('branches')):
            if isinstance(b, dict):
                if b.get('negate'):
                    part = b['regex'] + ' (negated)'
                else:
                    part = b['regex']
            else:
                part = b
            parts.append(part)
        return ' '.join(parts)

    def fromYaml(self, conf,
                 project_pipeline=False, name=None, validate=True):
        if validate:
            self.schema(conf)

        if name is None:
            name = conf['name']

        # NB: The default detection system in the Job class requires
        # that we always assign values directly rather than modifying
        # them (e.g., "job.run = ..." rather than
        # "job.run.append(...)").

        job = model.Job(name)
        job.description = conf.get('description')
        job.source_context = conf['_source_context']
        job.start_mark = conf['_start_mark']
        job.project_pipeline = project_pipeline
        job.variant_description = JobParser._makeVariantDescription(conf)

        if 'parent' in conf:
            if conf['parent'] is not None:
                # Parent job is explicitly specified, so inherit from it.
                job.parent = conf['parent']
            else:
                # Parent is explicitly set as None, so user intends
                # this to be a base job.
                job.parent = job.BASE_JOB_MARKER

        # Secrets are part of the playbook context so we must establish
        # them earlier than playbooks.
        secrets = []
        for secret_config in as_list(conf.get('secrets', [])):
            if isinstance(secret_config, str):
                secret_name = secret_config
                secret_alias = secret_config
                secret_ptp = False
            else:
                secret_name = secret_config['secret']
                secret_alias = secret_config['name']
                secret_ptp = secret_config.get('pass-to-parent', False)
            secret_use = model.SecretUse(secret_name, secret_alias)
            secret_use.pass_to_parent = secret_ptp
            secrets.append(secret_use)
        job.secrets = tuple(secrets)

        if 'post-review' in conf:
            if conf['post-review']:
                job.post_review = True
            else:
                raise Exception("Once set, the post-review attribute "
                                "may not be unset")

        job.ansible_split_streams = conf.get('ansible-split-streams')

        # Configure and validate ansible version
        if 'ansible-version' in conf:
            # The ansible-version can be treated by yaml as a float or
            # int so convert it to a string.
            ansible_version = str(conf['ansible-version'])
            self.pcontext.ansible_manager.requestVersion(ansible_version)
            job.ansible_version = ansible_version

        # Roles are part of the playbook context so we must establish
        # them earlier than playbooks.
        roles = []
        if 'roles' in conf:
            for role in conf.get('roles', []):
                if 'zuul' in role:
                    r = self._makeZuulRole(role)
                    if r:
                        roles.append(r)
        # A job's repo should be an implicit role source for that job,
        # but not in a project-pipeline variant.
        if not project_pipeline:
            r = self._makeImplicitRole(job)
            roles.insert(0, r)
        if roles:
            job.roles = tuple(roles)

        seen_playbook_semaphores = set()

        def sorted_semaphores(pb_dict):
            pb_semaphore_names = as_list(pb_dict.get('semaphores', []))
            seen_playbook_semaphores.update(pb_semaphore_names)
            pb_semaphores = [model.JobSemaphore(x) for x in pb_semaphore_names]
            return tuple(sorted(pb_semaphores,
                                key=lambda x: x.name))

        for pb_dict in [dict_with_name(x) for x in
                        as_list(conf.get('pre-run'))]:
            pre_run = model.PlaybookContext(job.source_context,
                                            pb_dict['name'], job.roles,
                                            secrets,
                                            sorted_semaphores(pb_dict))
            job.pre_run = job.pre_run + (pre_run,)
        # NOTE(pabelanger): Reverse the order of our post-run list. We prepend
        # post-runs for inherits however, we want to execute post-runs in the
        # order they are listed within the job.
        for pb_dict in [dict_with_name(x) for x in
                        reversed(as_list(conf.get('post-run')))]:
            post_run = model.PlaybookContext(job.source_context,
                                             pb_dict['name'], job.roles,
                                             secrets,
                                             sorted_semaphores(pb_dict),
                                             pb_dict.get('cleanup', False))
            job.post_run = (post_run,) + job.post_run

        with self.pcontext.confAttr(conf, 'cleanup-run') as cleanup_run:
            for pb_dict in [dict_with_name(x) for x in
                            reversed(as_list(cleanup_run))]:
                self.pcontext.accumulator.addError(CleanupRunDeprecation())
                cleanup_run = model.PlaybookContext(job.source_context,
                                                    pb_dict['name'], job.roles,
                                                    secrets,
                                                    sorted_semaphores(pb_dict))
                job.cleanup_run = (cleanup_run,) + job.cleanup_run

        if 'run' in conf:
            for pb_dict in [dict_with_name(x) for x in
                            as_list(conf.get('run'))]:
                run = model.PlaybookContext(job.source_context,
                                            pb_dict['name'], job.roles,
                                            secrets,
                                            sorted_semaphores(pb_dict))
                job.run = job.run + (run,)

        if conf.get('intermediate', False) and not conf.get('abstract', False):
            raise Exception("An intermediate job must also be abstract")

        for k in self.simple_attributes:
            a = k.replace('-', '_')
            if k in conf:
                setattr(job, a, conf[k])
        if 'nodeset' in conf:
            conf_nodeset = conf['nodeset']
            if isinstance(conf_nodeset, str):
                # This references an existing named nodeset in the
                # layout; it will be validated later.
                ns = conf_nodeset
            else:
                ns = self.pcontext.nodeset_parser.fromYaml(
                    conf_nodeset, anonymous=True)
            job.nodeset = ns

        if 'required-projects' in conf:
            with self.pcontext.confAttr(
                    conf, 'required-projects') as conf_projects:
                if isinstance(conf_projects, yaml.OverrideValue):
                    job.override_control['required_projects'] =\
                        conf_projects.override
                    conf_projects = conf_projects.value
                new_projects = {}
                projects = as_list(conf_projects)
                for project in projects:
                    if isinstance(project, dict):
                        project_name = project['name']
                        project_override_branch = project.get(
                            'override-branch')
                        project_override_checkout = project.get(
                            'override-checkout')
                    else:
                        project_name = project
                        project_override_branch = None
                        project_override_checkout = None
                    job_project = model.JobProject(project_name,
                                                   project_override_branch,
                                                   project_override_checkout)
                    new_projects[project_name] = job_project

                job.required_projects = new_projects

        for attr in ('include', 'exclude'):
            if f'{attr}-projects' in conf:
                with self.pcontext.confAttr(
                        conf, f'{attr}-projects') as conf_projects:
                    if isinstance(conf_projects, yaml.OverrideValue):
                        job.override_control[f'{attr}_projects'] =\
                            conf_projects.override
                        conf_projects = conf_projects.value
                    if conf_projects is None:
                        new_projects = None
                    else:
                        new_projects = set()
                        for conf_project in as_list(conf_projects):
                            if isinstance(conf_project, str):
                                conf_project = {
                                    'type': 'name',
                                    'name': conf_project,
                                }
                            conf_project = (
                                conf_project['type'],
                                conf_project.get('name'),
                            )
                            new_projects.add(conf_project)
                    setattr(job, f'{attr}_projects', new_projects)

        if 'dependencies' in conf:
            with self.pcontext.confAttr(conf, 'dependencies') as conf_deps:
                if isinstance(conf_deps, yaml.OverrideValue):
                    job.override_control['dependencies'] = conf_deps.override
                    conf_deps = conf_deps.value
                new_dependencies = []
                dependencies = as_list(conf_deps)
                for dep in dependencies:
                    if isinstance(dep, dict):
                        dep_name = dep['name']
                        dep_soft = dep.get('soft', False)
                    else:
                        dep_name = dep
                        dep_soft = False
                    job_dependency = model.JobDependency(dep_name, dep_soft)
                    new_dependencies.append(job_dependency)
                job.dependencies = frozenset(new_dependencies)

        with self.pcontext.confAttr(conf, 'semaphore') as conf_semaphore:
            semaphores = as_list(conf_semaphore)
        if 'semaphores' in conf:
            with self.pcontext.confAttr(conf, 'semaphores') as conf_semaphores:
                semaphores = as_list(conf_semaphores)
        # TODO: after removing semaphore, indent this section
        job_semaphores = []
        for semaphore in semaphores:
            if isinstance(semaphore, str):
                job_semaphores.append(model.JobSemaphore(semaphore))
            else:
                job_semaphores.append(model.JobSemaphore(
                    semaphore.get('name'),
                    semaphore.get('resources-first', False)))

        if job_semaphores:
            # Sort the list of semaphores to avoid issues with
            # contention (where two jobs try to start at the same time
            # and fail due to acquiring the same semaphores but in
            # reverse order.
            job.semaphores = tuple(sorted(job_semaphores,
                                          key=lambda x: x.name))
            common = (set([x.name for x in job_semaphores]) &
                      seen_playbook_semaphores)
            if common:
                raise Exception(f"Semaphores {common} specified as both "
                                "job and playbook semaphores but may only "
                                "be used for one")

        for k in ('tags', 'requires', 'provides'):
            with self.pcontext.confAttr(conf, k) as conf_k:
                if isinstance(conf_k, yaml.OverrideValue):
                    job.override_control[k] = conf_k.override
                    conf_k = conf_k.value
                v = frozenset(as_list(conf_k))
                if v:
                    setattr(job, k, v)

        for (yaml_attr, job_attr, hostvars) in [
                ('vars', 'variables', False),
                ('extra-vars', 'extra_variables', False),
                ('host-vars', 'host_variables', True),
                ('group-vars', 'group_variables', True),
        ]:
            with self.pcontext.confAttr(conf, yaml_attr) as conf_vars:
                if isinstance(conf_vars, yaml.OverrideValue):
                    job.override_control[job_attr] = conf_vars.override
                    conf_vars = conf_vars.value
                if conf_vars:
                    if hostvars:
                        for host, hvars in conf_vars.items():
                            check_varnames(hvars)
                    else:
                        check_varnames(conf_vars)
                    setattr(job, job_attr, conf_vars)

        with self.pcontext.confAttr(conf, 'include-vars') as conf_include_vars:
            if isinstance(conf_include_vars, yaml.OverrideValue):
                job.override_control['include_vars'] =\
                    conf_include_vars.override
                conf_include_vars = conf_include_vars.value
            if conf_include_vars:
                include_vars = []
                for iv in as_list(conf_include_vars):
                    if isinstance(iv, str):
                        iv = {'name': iv}
                    pname = None
                    if not iv.get('zuul-project', False):
                        pname = iv.get(
                            'project',
                            conf['_source_context'].project_canonical_name)
                    job_include_vars = model.JobIncludeVars(
                        iv['name'],
                        pname,
                        iv.get('required', True),
                        iv.get('use-ref', True),
                    )
                    include_vars.append(job_include_vars)
                job.include_vars = include_vars

        allowed_projects = conf.get('allowed-projects', None)
        # See note above at "post-review".
        if allowed_projects and not job.allowed_projects:
            job.allowed_projects = set(as_list(allowed_projects))

        branches = None
        if 'branches' in conf:
            with self.pcontext.confAttr(conf, 'branches') as conf_branches:
                branches = [
                    change_matcher.BranchMatcher(
                        make_regex(x, self.pcontext))
                    for x in as_list(conf_branches)
                ]
                job.setExplicitBranchMatchers(branches)
        elif not project_pipeline:
            branches = self.pcontext.getImpliedBranches(job.source_context)
            job.setImpliedBranchMatchers(branches)
        if 'files' in conf:
            with self.pcontext.confAttr(conf, 'files') as conf_files:
                if isinstance(conf_files, yaml.OverrideValue):
                    job.override_control['file_matcher'] = conf_files.override
                    conf_files = conf_files.value
                job.setFileMatcher([
                    make_regex(x, self.pcontext)
                    for x in as_list(conf_files)
                ])
        if 'irrelevant-files' in conf:
            with self.pcontext.confAttr(conf,
                                        'irrelevant-files') as conf_ifiles:
                if isinstance(conf_ifiles, yaml.OverrideValue):
                    job.override_control['irrelevant_file_matcher'] =\
                        conf_ifiles.override
                    conf_ifiles = conf_ifiles.value
                job.setIrrelevantFileMatcher([
                    make_regex(x, self.pcontext)
                    for x in as_list(conf_ifiles)
                ])
        if 'failure-output' in conf:
            with self.pcontext.confAttr(conf,
                                        'failure-output') as conf_output:
                if isinstance(conf_output, yaml.OverrideValue):
                    job.override_control['failure_output'] =\
                        conf_output.override
                    conf_output = conf_output.value
            failure_output = as_list(conf_output)
            # Test compilation to detect errors, but the zuul_stream
            # callback plugin is what actually needs re objects, so we
            # let it recompile them later.
            for x in failure_output:
                re2.compile(x)
            job.failure_output = tuple(sorted(failure_output))

        if 'attribute-control' in conf:
            with self.pcontext.confAttr(conf,
                                        'attribute-control') as conf_control:
                for attr, controls in conf_control.items():
                    jobattr = self._getAttrControlJobAttr(attr)
                    for control, value in controls.items():
                        if control == 'final' and value is True:
                            job.final_control[jobattr] = True
        return job

    def _makeZuulRole(self, role):
        name = role['zuul'].split('/')[-1]
        return model.ZuulRole(role.get('name', name),
                              role['zuul'])

    def _makeImplicitRole(self, job):
        project_name = job.source_context.project_name
        name = project_name.split('/')[-1]
        name = JobParser.ANSIBLE_ROLE_RE.sub('', name) or name
        return model.ZuulRole(name,
                              job.source_context.project_canonical_name,
                              implicit=True)


class ProjectTemplateParser(object):
    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.ProjectTemplateParser")
        self.pcontext = pcontext
        self.schema = self.getSchema()
        self.not_pipelines = ['name', 'description', 'templates',
                              'merge-mode', 'default-branch', 'vars',
                              'queue', 'branches',
                              '_source_context', '_start_mark']

    def getSchema(self):
        job = {str: vs.Any(str, JobParser.job_attributes)}
        job_list = [vs.Any(str, job)]

        parameter_types = vs.Any(str, int, float, bool)

        parameter = {
            vs.Required('name'): ansible_var_name,
            vs.Required('type'): vs.Any(
                'string', 'selection', 'multiple-selection', 'bool'),
            'description': str,
            'values': [parameter_types],
            'default': to_list(parameter_types),
            'required': bool,
        }

        pipeline_contents = {
            'debug': bool,
            'fail-fast': bool,
            'jobs': job_list,
            'parameters': to_list(parameter),
        }

        project = {
            'name': str,
            'description': str,
            'queue': str,
            'vars': ansible_vars_dict,
            str: pipeline_contents,
            '_source_context': model.SourceContext,
            '_start_mark': model.ZuulMark,
        }

        return vs.Schema(project)

    def fromYaml(self, conf, validate=True):
        if validate:
            self.schema(conf)
        source_context = conf['_source_context']
        start_mark = conf['_start_mark']
        project_template = model.ProjectConfig(conf.get('name'))
        project_template.source_context = conf['_source_context']
        project_template.start_mark = conf['_start_mark']
        project_template.is_template = True
        project_template.queue_name = conf.get('queue')
        for pipeline_name, conf_pipeline in conf.items():
            if pipeline_name in self.not_pipelines:
                continue
            project_pipeline = model.ProjectPipelineConfig()
            project_template.pipelines[pipeline_name] = project_pipeline
            project_pipeline.debug = conf_pipeline.get('debug')
            project_pipeline.fail_fast = conf_pipeline.get(
                'fail-fast')
            self.parseJobList(
                conf_pipeline.get('jobs', []),
                source_context, start_mark, project_pipeline.job_list)
            for conf_param in conf_pipeline.get('parameters', []):
                param = model.ProjectParameterConfig()
                param.name = conf_param['name']
                param.type = conf_param['type']
                param.description = conf_param.get('description')
                param.values = conf_param.get('values')
                param.default = conf_param.get('default')
                param.required = conf_param.get('required', False)
                project_pipeline.parameters[param.name] = param

        # If this project definition is in a place where it
        # should get implied branch matchers, set it.
        branches = self.pcontext.getImpliedBranches(source_context)
        if branches:
            project_template.setImpliedBranchMatchers(branches)

        variables = conf.get('vars', {})
        if variables:
            if forbidden := set(variables.keys()).intersection(
                    varnames.RESERVED_NAMES):
                forbidden = ','.join(list(forbidden))
                raise Exception(f"Variables named {forbidden} "
                                "are not allowed.")
            project_template.variables = variables

        return project_template

    def parseJobList(self, conf, source_context, start_mark, job_list):
        for conf_job in conf:
            if isinstance(conf_job, str):
                jobname = conf_job
                attrs = {}
            elif isinstance(conf_job, dict):
                # A dictionary in a job tree may override params
                jobname, attrs = list(conf_job.items())[0]
            else:
                raise Exception("Job must be a string or dictionary")
            attrs['_source_context'] = source_context
            attrs['_start_mark'] = start_mark

            job_list.addJob(self.pcontext.job_parser.fromYaml(
                attrs, project_pipeline=True,
                name=jobname, validate=False))


class ProjectParser(object):
    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.ProjectParser")
        self.pcontext = pcontext
        self.schema = self.getSchema()

    def getSchema(self):
        job = {str: vs.Any(str, JobParser.job_attributes)}
        job_list = [vs.Any(str, job)]

        parameter_types = vs.Any(str, int, float, bool)

        parameter = {
            vs.Required('name'): ansible_var_name,
            vs.Required('type'): vs.Any(
                'string', 'selection', 'multiple-selection', 'bool'),
            'description': str,
            'values': [parameter_types],
            'default': to_list(parameter_types),
            'required': bool,
        }

        pipeline_contents = {
            'debug': bool,
            'fail-fast': bool,
            'jobs': job_list,
            'parameters': to_list(parameter),
        }

        project = {
            'name': str,
            'description': str,
            'branches': to_list(vs.Any(ZUUL_REGEX, str)),
            'vars': ansible_vars_dict,
            'templates': [str],
            'merge-mode': vs.Any('merge', 'merge-resolve',
                                 'cherry-pick', 'squash-merge',
                                 'rebase'),
            'default-branch': str,
            'queue': str,
            str: pipeline_contents,
            '_source_context': model.SourceContext,
            '_start_mark': model.ZuulMark,
        }

        return vs.Schema(project)

    def fromYaml(self, conf):
        self.schema(conf)

        project_name = conf.get('name')
        source_context = conf['_source_context']
        if not project_name:
            # There is no name defined so implicitly add the name
            # of the project where it is defined.
            project_name = (source_context.project_canonical_name)

        # Parse the project as a template since they're mostly the
        # same.
        project_config = self.pcontext.project_template_parser. \
            fromYaml(conf, validate=False)

        project_config.name = project_name

        # Explicitly override this to False since we're reusing the
        # project-template loading method which sets it True.
        project_config.is_template = False

        branches = None
        if 'branches' in conf:
            with self.pcontext.confAttr(conf, 'branches') as conf_branches:
                branches = [
                    change_matcher.BranchMatcher(
                        make_regex(x, self.pcontext))
                    for x in as_list(conf_branches)
                ]
        if branches:
            project_config.setExplicitBranchMatchers(branches)

        # Always set the source implied branch matcher so it's
        # available if needed.
        project_config.setSourceImpliedBranchMatchers(
            [change_matcher.ImpliedBranchMatcher(
                ZuulRegex(source_context.branch))])

        # Add templates
        for name in conf.get('templates', []):
            if name not in project_config.templates:
                project_config.templates.append(name)

        mode = conf.get('merge-mode')
        if mode is not None:
            project_config.merge_mode = model.MERGER_MAP[mode]

        default_branch = conf.get('default-branch')
        project_config.default_branch = default_branch

        project_config.queue_name = conf.get('queue', None)

        variables = conf.get('vars', {})
        if variables:
            if forbidden := set(variables.keys()).intersection(
                    varnames.RESERVED_NAMES):
                forbidden = ','.join(list(forbidden))
                raise Exception(f"Variables named {forbidden} "
                                "are not allowed.")
            project_config.variables = variables

        return project_config


class PipelineParser(object):
    # A set of reporter configuration keys to action mapping
    reporter_actions = {
        'enqueue': 'enqueue_actions',
        'start': 'start_actions',
        'success': 'success_actions',
        'failure': 'failure_actions',
        'merge-conflict': 'merge_conflict_actions',
        'config-error': 'config_error_actions',
        'no-jobs': 'no_jobs_actions',
        'disabled': 'disabled_actions',
        'dequeue': 'dequeue_actions',
    }

    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.PipelineParser")
        self.pcontext = pcontext
        self.schema = self.getSchema()

    def getDriverSchema(self, dtype):
        methods = {
            'trigger': 'getTriggerSchema',
            'reporter': 'getReporterSchema',
            'require': 'getRequireSchema',
            'reject': 'getRejectSchema',
        }

        schema = {}
        # Add the configured connections as available layout options
        for connection_name, connection in \
            self.pcontext.connections.connections.items():
            method = getattr(connection.driver, methods[dtype], None)
            if method:
                schema[connection_name] = to_list(method())

        return schema

    def getSchema(self):
        manager = vs.Any('independent',
                         'dependent',
                         'serial',
                         'supercedent')

        precedence = vs.Any('normal', 'low', 'high')

        window = vs.All(int, vs.Range(min=0))
        window_floor = vs.All(int, vs.Range(min=1))
        window_ceiling = vs.Any(None, vs.All(int, vs.Range(min=1)))
        window_type = vs.Any('linear', 'exponential')
        window_factor = vs.All(int, vs.Range(min=1))

        pipeline = {vs.Required('name'): str,
                    vs.Required('manager'): manager,
                    'allow-other-connections': bool,
                    'allow-parameters': bool,
                    'precedence': precedence,
                    'supercedes': to_list(str),
                    'description': str,
                    'success-message': str,
                    'failure-message': str,
                    'start-message': str,
                    'merge-conflict-message': str,
                    'enqueue-message': str,
                    'no-jobs-message': str,
                    'footer-message': str,
                    'dequeue-message': str,
                    'dequeue-on-new-patchset': bool,
                    'ignore-dependencies': bool,
                    'post-review': bool,
                    'disable-after-consecutive-failures':
                        vs.All(int, vs.Range(min=1)),
                    'window': window,
                    'window-floor': window_floor,
                    'window-ceiling': window_ceiling,
                    'window-increase-type': window_type,
                    'window-increase-factor': window_factor,
                    'window-decrease-type': window_type,
                    'window-decrease-factor': window_factor,
                    '_source_context': model.SourceContext,
                    '_start_mark': model.ZuulMark,
                    }
        pipeline['require'] = self.getDriverSchema('require')
        pipeline['reject'] = self.getDriverSchema('reject')
        pipeline['trigger'] = vs.Required(self.getDriverSchema('trigger'))
        for action in ['enqueue', 'start', 'success', 'failure',
                       'merge-conflict', 'no-jobs', 'disabled',
                       'dequeue', 'config-error']:
            pipeline[action] = self.getDriverSchema('reporter')
        return vs.Schema(pipeline)

    def fromYaml(self, conf):
        self.schema(conf)
        pipeline = model.Pipeline(conf['name'])
        pipeline.source_context = conf['_source_context']
        pipeline.start_mark = conf['_start_mark']
        pipeline.allow_other_connections = conf.get(
            'allow-other-connections', True)
        pipeline.allow_parameters = conf.get(
            'allow-parameters', False)
        pipeline.description = conf.get('description')
        pipeline.supercedes = as_list(conf.get('supercedes', []))

        precedence = model.PRECEDENCE_MAP[conf.get('precedence')]
        pipeline.precedence = precedence
        pipeline.failure_message = conf.get('failure-message',
                                            "Build failed.")
        pipeline.merge_conflict_message = conf.get(
            'merge-conflict-message', "Merge Failed.\n\nThis change or one "
            "of its cross-repo dependencies was unable to be "
            "automatically merged with the current state of its "
            "repository. Please rebase the change and upload a new "
            "patchset.")

        pipeline.success_message = conf.get('success-message',
                                            "Build succeeded.")
        pipeline.footer_message = conf.get('footer-message', "")
        pipeline.start_message = conf.get('start-message',
                                          "Starting {pipeline.name} jobs.")
        pipeline.enqueue_message = conf.get('enqueue-message', "")
        pipeline.no_jobs_message = conf.get('no-jobs-message', "")
        pipeline.dequeue_message = conf.get(
            "dequeue-message", "Build canceled."
        )
        pipeline.dequeue_on_new_patchset = conf.get(
            'dequeue-on-new-patchset', True)
        pipeline.ignore_dependencies = conf.get(
            'ignore-dependencies', False)
        pipeline.post_review = conf.get(
            'post-review', False)

        # TODO: Remove in Zuul v6.0
        # Make a copy to manipulate for backwards compat.
        conf_copy = conf.copy()

        seen_connections = set()
        for conf_key, action in self.reporter_actions.items():
            reporter_set = []
            if conf_copy.get(conf_key):
                for reporter_name, params in conf_copy.get(conf_key).items():
                    reporter = self.pcontext.connections.getReporter(
                        reporter_name, pipeline, params, self.pcontext)
                    reporter.setAction(conf_key)
                    reporter_set.append(reporter)
                    seen_connections.add(reporter_name)
                    if (reporter_name == 'zuul' and
                        getattr(reporter, 'image_built', False)):
                        pipeline.builds_images = True
            setattr(pipeline, action, reporter_set)

        # If merge-conflict actions aren't explicit, use the failure actions
        if not pipeline.merge_conflict_actions:
            pipeline.merge_conflict_actions = pipeline.failure_actions

        # If config-error actions aren't explicit, use the failure actions
        if not pipeline.config_error_actions:
            pipeline.config_error_actions = pipeline.failure_actions

        pipeline.disable_at = conf.get(
            'disable-after-consecutive-failures', None)

        pipeline.window = conf.get('window', 20)
        pipeline.window_floor = conf.get('window-floor', 3)
        pipeline.window_ceiling = conf.get('window-ceiling', None)
        if (pipeline.window_ceiling is None):
            pipeline.window_ceiling = math.inf
        if pipeline.window_ceiling < pipeline.window_floor:
            raise Exception("Pipeline window-ceiling may not be "
                            "less than window-floor")
        pipeline.window_increase_type = conf.get(
            'window-increase-type', 'linear')
        pipeline.window_increase_factor = conf.get(
            'window-increase-factor', 1)
        pipeline.window_decrease_type = conf.get(
            'window-decrease-type', 'exponential')
        pipeline.window_decrease_factor = conf.get(
            'window-decrease-factor', 2)

        pipeline.manager_name = conf['manager']

        with self.pcontext.errorContext(stanza='pipeline', conf=conf):
            with self.pcontext.confAttr(conf, 'require', {}) as require_dict:
                for source_name, require_config in require_dict.items():
                    source = self.pcontext.connections.getSource(source_name)
                    pipeline.ref_filters.extend(
                        source.getRequireFilters(
                            require_config, self.pcontext))
                    seen_connections.add(source_name)

            with self.pcontext.confAttr(conf, 'reject', {}) as reject_dict:
                for source_name, reject_config in reject_dict.items():
                    source = self.pcontext.connections.getSource(source_name)
                    pipeline.ref_filters.extend(
                        source.getRejectFilters(reject_config, self.pcontext))
                    seen_connections.add(source_name)

            with self.pcontext.confAttr(conf, 'trigger', {}) as trigger_dict:
                for connection_name, trigger_config in trigger_dict.items():
                    trigger = self.pcontext.connections.getTrigger(
                        connection_name, trigger_config)
                    pipeline.triggers.append(trigger)
                    pipeline.event_filters.extend(
                        trigger.getEventFilters(
                            connection_name,
                            conf['trigger'][connection_name],
                            self.pcontext))
                    seen_connections.add(connection_name)

        pipeline.connections = list(seen_connections)
        # Pipelines don't get frozen
        return pipeline


class SemaphoreParser(object):
    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.SemaphoreParser")
        self.pcontext = pcontext
        self.schema = self.getSchema()

    def getSchema(self):
        semaphore = {vs.Required('name'): str,
                     'max': int,
                     '_source_context': model.SourceContext,
                     '_start_mark': model.ZuulMark,
                     }

        return vs.Schema(semaphore)

    def fromYaml(self, conf):
        self.schema(conf)
        semaphore = model.Semaphore(conf['name'], conf.get('max', 1))
        semaphore.source_context = conf.get('_source_context')
        semaphore.start_mark = conf.get('_start_mark')
        return semaphore


class QueueParser:
    def __init__(self, pcontext):
        self.log = logging.getLogger("zuul.QueueParser")
        self.pcontext = pcontext
        self.schema = self.getSchema()

    def getSchema(self):
        queue = {vs.Required('name'): str,
                 vs.Exclusive('per-branch', 'type'): bool,
                 'allow-circular-dependencies': bool,
                 'dependencies-by-topic': bool,
                 vs.Exclusive('type', 'type'): vs.Any(
                     'all-branches', 'per-branch', 'branch-assigned'),
                 '_source_context': model.SourceContext,
                 '_start_mark': model.ZuulMark,
                 }
        return vs.Schema(queue)

    def fromYaml(self, conf):
        self.schema(conf)
        # Default or explicit value
        qtype = conf.get('type', 'all-branches')
        # If per-branch was specified, then type wasn't
        if conf.get('per-branch'):
            qtype = 'per-branch'
        queue = model.Queue(
            conf['name'],
            conf.get('allow-circular-dependencies', False),
            conf.get('dependencies-by-topic', False),
            qtype,
        )
        if (queue.dependencies_by_topic and not
            queue.allow_circular_dependencies):
            raise Exception("The 'allow-circular-dependencies' setting must be"
                            "enabled in order to use dependencies-by-topic")
        queue.source_context = conf.get('_source_context')
        queue.start_mark = conf.get('_start_mark')
        return queue


class AuthorizationRuleParser(object):
    def __init__(self):
        self.log = logging.getLogger("zuul.AuthorizationRuleParser")
        self.schema = self.getSchema()

    def getSchema(self):
        authRule = {vs.Required('name'): str,
                    vs.Required('conditions'): to_list(dict)
                   }
        return vs.Schema(authRule)

    def fromYaml(self, conf):
        self.schema(conf)
        a = model.AuthZRuleTree(conf['name'])

        def parse_tree(node):
            if isinstance(node, list):
                return model.OrRule(parse_tree(x) for x in node)
            elif isinstance(node, dict):
                subrules = []
                for claim, value in node.items():
                    if claim == 'zuul_uid':
                        claim = '__zuul_uid_claim'
                    subrules.append(model.ClaimRule(claim, value))
                return model.AndRule(subrules)
            else:
                raise Exception('Invalid claim declaration %r' % node)

        a.ruletree = parse_tree(conf['conditions'])
        return a


class AuthorizationRoleParser(object):
    read = {
        'read': bool
    }
    promote = {
        'promote': bool
    }
    set_tenant_state = {
        'set-tenant-state': bool
    }
    build_image = {
        'build-image': bool
    }
    upload_image = {
        'upload-image': bool
    }
    delete_image_build_artifact = {
        'delete-image-build-artifact': bool
    }
    delete_image_upload = {
        'delete-image-upload': bool
    }
    validate_image_uload = {
        'validate-image-upload': bool
    }
    modify_node = {
        'modify-node': bool
    }
    modify_nodeset_request = {
        'modify-nodeset-request': bool
    }
    autohold = {
        'autohold': bool
    }
    # This construction comes from:
    # https://github.com/alecthomas/voluptuous/issues/126#issuecomment-134322625
    dequeue = {
        'dequeue':
            vs.Any(bool, {
                'conditions': vs.All(
                    # Validate conditions keys
                    vs.Schema({vs.Required(vs.Any('project', 'ref')): object}),
                    # Validate conditions data
                    vs.Schema({
                        vs.Optional('project'): str,
                        vs.Optional('ref'): str
                    })
                )
            })
    }
    enqueue = {
        'enqueue':
            vs.Any(bool, {
                'conditions': vs.All(
                    # Validate conditions keys
                    vs.Schema({vs.Required(vs.Any('project', 'ref')): object}),
                    # Validate conditions data
                    vs.Schema({
                        vs.Optional('project'): str,
                        vs.Optional('ref'): str
                    })
                )
            })
    }

    def __init__(self):
        self.log = logging.getLogger("zuul.AuthorizationRoleParser")
        self.schema = self.getSchema()

    def getSchema(self):
        permissions = {}
        permissions.update(self.read)
        permissions.update(self.promote)
        permissions.update(self.set_tenant_state)
        permissions.update(self.build_image)
        permissions.update(self.upload_image)
        permissions.update(self.delete_image_build_artifact)
        permissions.update(self.delete_image_upload)
        permissions.update(self.modify_node)
        permissions.update(self.modify_nodeset_request)
        permissions.update(self.autohold)
        permissions.update(self.dequeue)
        permissions.update(self.enqueue)
        authRole = {
            vs.Required('name'): vs.All(str, vs.NotIn(('read', 'admin'))),
            vs.Required('permissions'): permissions,
        }
        return vs.Schema(authRole)

    def fromYaml(self, conf):
        self.schema(conf)
        a = model.AuthZConfigRole(conf)
        return a


class GlobalSemaphoreParser(object):
    def __init__(self):
        self.log = logging.getLogger("zuul.GlobalSemaphoreParser")
        self.schema = self.getSchema()

    def getSchema(self):
        semaphore = {vs.Required('name'): str,
                     'max': int,
                     }

        return vs.Schema(semaphore)

    def fromYaml(self, conf):
        self.schema(conf)
        semaphore = model.Semaphore(conf['name'], conf.get('max', 1),
                                    global_scope=True)
        return semaphore


class ApiRootParser(object):
    def __init__(self):
        self.log = logging.getLogger("zuul.ApiRootParser")
        self.schema = self.getSchema()

    def validateRoleMappings(self, abide, role_mappings):
        # This is not implemented as a validator because it requires
        # the abide.
        for authz_rule in role_mappings.keys():
            if authz_rule not in abide.authz_rules:
                raise AuthZRuleNotFoundError(authz_rule)
        for authz_roles in role_mappings.values():
            for authz_role in authz_roles:
                if authz_role not in abide.authz_roles:
                    raise AuthZRoleNotFoundError(authz_role)

    def getSchema(self):
        api_root = {
            'authentication-realm': str,
            'access-rules': to_list(str),
            'anonymous-read-access': bool,
            'role-mappings': {str: to_list(str)},
        }
        return vs.Schema(api_root)

    def fromYaml(self, abide, conf):
        self.schema(conf)
        api_root = model.ApiRoot(conf.get('authentication-realm'))
        api_root.access_rules = conf.get('access-rules', [])
        api_root.anonymous_read_access = conf.get(
            'anonymous-read-access', True)
        api_root.role_mappings = {}
        if conf.get('role-mappings') is not None:
            for key, val in conf['role-mappings'].items():
                api_root.role_mappings[key] = as_list(val)
            self.validateRoleMappings(abide, api_root.role_mappings)
        return api_root


class ParseContext(object):
    """Hold information about a particular run of the parser"""

    def __init__(self, connections, zuul_globals, system, ansible_manager):
        self.connections = connections
        self.globals = zuul_globals
        self.system = system
        self.ansible_manager = ansible_manager
        self.pragma_parser = PragmaParser(self)
        self.pipeline_parser = PipelineParser(self)
        self.nodeset_parser = NodeSetParser(self)
        self.secret_parser = SecretParser(self)
        self.job_parser = JobParser(self)
        self.semaphore_parser = SemaphoreParser(self)
        self.queue_parser = QueueParser(self)
        self.image_parser = ImageParser(self)
        self.flavor_parser = FlavorParser(self)
        self.label_parser = LabelParser(self)
        self.section_parser = SectionParser(self)
        self.provider_parser = ProviderParser(self)
        self.project_template_parser = ProjectTemplateParser(self)
        self.project_parser = ProjectParser(self)
        self.error_list = []
        acc = LocalAccumulator(self.error_list)
        # Currently we use thread local storage to ensure that we
        # don't accidentally use the error context stack from one of
        # our threadpool workers.  In the future, we may be able to
        # refactor so that the workers branch it whenever they start
        # work.
        self._thread_local = threading.local()
        self.resetErrorContext(acc)

    def resetErrorContext(self, acc):
        self._thread_local.accumulators = [acc]

    @property
    def accumulator(self):
        return self._thread_local.accumulators[-1]

    @contextmanager
    def errorContext(self, error_list=None, source_context=None,
                     stanza=None, conf=None, attr=None):
        acc = self.accumulator.extend(error_list=error_list,
                                      source_context=source_context,
                                      stanza=stanza,
                                      conf=conf,
                                      attr=attr)
        self._thread_local.accumulators.append(acc)
        try:
            yield
        finally:
            if len(self._thread_local.accumulators) > 1:
                self._thread_local.accumulators.pop()

    @contextmanager
    def confAttr(self, conf, attr, default=None):
        found = None
        for k in conf.keys():
            if k == attr:
                found = k
                break
        if found is not None:
            with self.errorContext(attr=found):
                yield conf[found]
        else:
            yield default

    def getImpliedBranches(self, source_context):
        if source_context.implied_branches is not None:
            return source_context.implied_branches
        return [change_matcher.ImpliedBranchMatcher(
            ZuulRegex(source_context.branch))]


class TenantParser(object):
    def __init__(self, connections, zk_client, scheduler, merger, keystorage,
                 zuul_globals, statsd, unparsed_config_cache):
        self.log = logging.getLogger("zuul.TenantParser")
        self.connections = connections
        self.zk_client = zk_client
        self.scheduler = scheduler
        self.merger = merger
        self.keystorage = keystorage
        self.globals = zuul_globals
        self.statsd = statsd
        self.unparsed_config_cache = unparsed_config_cache

    job_classes = (
        'pipeline', 'job', 'semaphore', 'project',
        'project-template', 'nodeset', 'secret', 'queue',
    )
    provider_classes = (
        'image', 'flavor', 'label', 'section', 'provider',
    )
    all_classes = job_classes + provider_classes
    classes = vs.Any(*all_classes)

    inner_config_project_dict = {
        'include': to_list(classes),
        'exclude': to_list(classes),
        'shadow': to_list(str),
        'include-provider-config': bool,
        'exclude-unprotected-branches': bool,
        'exclude-locked-branches': bool,
        'extra-config-paths': no_dup_config_paths,
        'load-branch': str,
        'include-branches': to_list(str),
        'exclude-branches': to_list(str),
        'always-dynamic-branches': to_list(str),
        'allow-circular-dependencies': bool,
        'implied-branch-matchers': bool,
    }
    config_project_dict = {str: inner_config_project_dict}

    inner_untrusted_project_dict = inner_config_project_dict.copy()
    inner_untrusted_project_dict['configure-projects'] = to_list(str)
    inner_untrusted_project_dict['allow-reporter-jobs'] = bool
    untrusted_project_dict = {str: inner_untrusted_project_dict}

    config_project = vs.Any(str, config_project_dict)
    untrusted_project = vs.Any(str, untrusted_project_dict)

    config_group = {
        'include': to_list(classes),
        'exclude': to_list(classes),
        'include-provider-config': bool,
        vs.Required('projects'): to_list(config_project),
    }
    untrusted_group = {
        'include': to_list(classes),
        'exclude': to_list(classes),
        'include-provider-config': bool,
        vs.Required('projects'): to_list(untrusted_project),
    }

    config_project_or_group = vs.Any(config_project, config_group)
    untrusted_project_or_group = vs.Any(untrusted_project, untrusted_group)

    tenant_source = vs.Schema({
        'config-projects': to_list(config_project_or_group),
        'untrusted-projects': to_list(untrusted_project_or_group),
    })

    def validateTenantSources(self):
        def v(value, path=[]):
            if isinstance(value, dict):
                for k, val in value.items():
                    self.connections.getSource(k)
                    self.validateTenantSource(val, path + [k])
            else:
                raise vs.Invalid("Invalid tenant source", path)
        return v

    def validateTenantSource(self, value, path=[]):
        self.tenant_source(value)

    def validateGlobalSemaphores(self, abide, global_semaphores):
        # This is not implemented as a validator because it requires
        # the abide.
        for semaphore_name in global_semaphores:
            if semaphore_name not in abide.semaphores:
                raise GlobalSemaphoreNotFoundError(semaphore_name)

    def validateRoleMappings(self, abide, role_mappings):
        # This is not implemented as a validator because it requires
        # the abide.
        for authz_rule in role_mappings.keys():
            if authz_rule not in abide.authz_rules:
                raise AuthZRuleNotFoundError(authz_rule)
        for authz_roles in role_mappings.values():
            for authz_role in authz_roles:
                if authz_role not in abide.authz_roles:
                    raise AuthZRoleNotFoundError(authz_role)

    def getSchema(self):
        tenant = {vs.Required('name'): str,
                  'max-changes-per-pipeline': int,
                  'max-dependencies': int,
                  'max-nodes-per-job': int,
                  'max-job-timeout': int,
                  'max-oidc-ttl': int,
                  'allowed-oidc-issuers': to_list(str),
                  'source': self.validateTenantSources(),
                  'exclude-unprotected-branches': bool,
                  'exclude-locked-branches': bool,
                  'allowed-triggers': to_list(str),
                  'allowed-reporters': to_list(str),
                  'allowed-labels': to_list(str),
                  'disallowed-labels': to_list(str),
                  'allow-circular-dependencies': bool,
                  'default-parent': str,
                  'default-ansible-version': vs.Any(str, float, int),
                  'access-rules': to_list(str),
                  'admin-rules': to_list(str),
                  'anonymous-read-access': bool,
                  'role-mappings': {str: to_list(str)},
                  'semaphores': to_list(str),
                  'authentication-realm': str,
                  # TODO: Ignored, allowed for backwards compat, remove for v5.
                  'report-build-page': bool,
                  # TODO: Remove after nodepool transitions
                  'use-nodepool': bool,
                  'web-root': str,
                  }
        return vs.Schema(tenant)

    def fromYaml(self, abide, conf, ansible_manager, executor, system,
                 min_ltimes=None, layout_uuid=None,
                 branch_cache_min_ltimes=None,
                 ignore_cat_exception=True):
        # Note: This vs schema validation is not necessary in most cases as we
        # verify the schema when loading tenant configs into zookeeper.
        # However, it is theoretically possible in a multi scheduler setup that
        # one scheduler would load the config into zk with validated schema
        # then another newer or older scheduler could load it from zk and fail.
        # We validate again to help users debug this situation should it
        # happen.
        self.getSchema()(conf)
        tenant = model.Tenant(conf['name'])
        pcontext = ParseContext(self.connections, self.globals,
                                system, ansible_manager)
        if conf.get('max-changes-per-pipeline') is not None:
            tenant.max_changes_per_pipeline = conf['max-changes-per-pipeline']
        if conf.get('max-dependencies') is not None:
            tenant.max_dependencies = conf['max-dependencies']
        if conf.get('max-nodes-per-job') is not None:
            tenant.max_nodes_per_job = conf['max-nodes-per-job']
        if conf.get('max-job-timeout') is not None:
            tenant.max_job_timeout = int(conf['max-job-timeout'])
        if conf.get('max-oidc-ttl') is not None:
            tenant.max_oidc_ttl = int(conf['max-oidc-ttl'])
        if conf.get('default-oidc-ttl') is not None:
            tenant.default_oidc_ttl = int(conf['default-oidc-ttl'])
        if conf.get('allowed-oidc-issuers') is not None:
            tenant.allowed_oidc_issuers = as_list(conf['allowed-oidc-issuers'])
        if conf.get('exclude-unprotected-branches') is not None:
            tenant.exclude_unprotected_branches = \
                conf['exclude-unprotected-branches']
        if conf.get('exclude-locked-branches') is not None:
            tenant.exclude_locked_branches = \
                conf['exclude-locked-branches']
        if conf.get('admin-rules') is not None:
            tenant.admin_rules = as_list(conf['admin-rules'])
        if conf.get('access-rules') is not None:
            tenant.access_rules = as_list(conf['access-rules'])
        tenant.anonymous_read_access = conf.get('anonymous-read-access', True)
        tenant.role_mappings = {}
        if conf.get('role-mappings') is not None:
            for key, val in conf['role-mappings'].items():
                tenant.role_mappings[key] = as_list(val)
            self.validateRoleMappings(abide, tenant.role_mappings)
        if conf.get('authentication-realm') is not None:
            tenant.default_auth_realm = conf['authentication-realm']
        if conf.get('semaphores') is not None:
            tenant.global_semaphores = set(as_list(conf['semaphores']))
            self.validateGlobalSemaphores(abide, tenant.global_semaphores)
        tenant.web_root = conf.get('web-root', self.globals.web_root)
        if tenant.web_root and not tenant.web_root.endswith('/'):
            tenant.web_root += '/'
        tenant.allowed_triggers = conf.get('allowed-triggers')
        tenant.allowed_reporters = conf.get('allowed-reporters')
        tenant.allowed_labels = conf.get('allowed-labels')
        tenant.disallowed_labels = conf.get('disallowed-labels')
        tenant.default_base_job = conf.get('default-parent', 'base')
        # TODO: switch default value after nodepool transition
        tenant.use_nodepool = conf.get('use-nodepool', True)

        keys_future = executor.submit(self._loadTenantKeys, tenant)

        tenant.unparsed_config = conf
        # tpcs is TenantProjectConfigs
        for tpc in abide.getAllTPCs(tenant.name):
            tenant.addTPC(tpc)

        # Get branches in parallel
        branch_futures = {}
        for tpc in abide.getAllTPCs(tenant.name):
            future = executor.submit(self._getProjectBranches,
                                     tenant, tpc, branch_cache_min_ltimes)
            branch_futures[future] = tpc

        for branch_future in as_completed(branch_futures.keys()):
            tpc = branch_futures[branch_future]
            source_context = model.SourceContext(
                tpc.project.canonical_name, tpc.project.name,
                tpc.project.connection_name, None, None)
            with pcontext.errorContext(source_context=source_context):
                with pcontext.accumulator.catchErrors():
                    # Call GPB again for the side effect of throwing
                    # exceptions if the project/branch is inacessible
                    self._getProjectBranches(tenant, tpc,
                                             branch_cache_min_ltimes)
                    self._resolveShadowProjects(tenant, tpc)

        # Set default ansible version
        default_ansible_version = conf.get('default-ansible-version')
        if default_ansible_version is not None:
            # The ansible version can be interpreted as float or int
            # by yaml so make sure it's a string.
            default_ansible_version = str(default_ansible_version)
            ansible_manager.requestVersion(default_ansible_version)
        else:
            default_ansible_version = ansible_manager.default_version
        tenant.default_ansible_version = default_ansible_version

        # Start by fetching any YAML needed by this tenant which isn't
        # already cached.  Full reconfigurations start with an empty
        # cache.
        self._cacheTenantYAML(abide, tenant, pcontext,
                              min_ltimes, executor, ignore_cat_exception)

        # Then collect the appropriate config objects based on this
        # tenant config.
        parsed_config = self._loadTenantParsedConfig(
            abide, tenant, pcontext)

        tenant.layout = self._parseLayout(
            tenant, parsed_config, pcontext, layout_uuid)

        tenant.semaphore_handler = SemaphoreHandler(
            self.zk_client, self.statsd, tenant.name, tenant.layout, abide,
            read_only=(not bool(self.scheduler))
        )
        keys_future.result()
        if self.scheduler:
            # Only call the postConfig hook if we have a scheduler as this will
            # change data in ZooKeeper. In case we are in a zuul-web context,
            # we don't want to do that.
            for manager in tenant.layout.pipeline_managers.values():
                manager._postConfig()

        return tenant

    def _resolveShadowProjects(self, tenant, tpc):
        shadow_projects = []
        for sp in tpc.shadow_projects:
            _, project = tenant.getProject(sp)
            if project is None:
                raise ProjectNotFoundError(sp)
            shadow_projects.append(project.canonical_name)
        tpc.shadow_projects = frozenset(shadow_projects)

    def _getProjectBranches(self, tenant, tpc, branch_cache_min_ltimes=None):
        if branch_cache_min_ltimes is not None:
            # Use try/except here instead of .get in order to allow
            # defaultdict to supply a default other than our default
            # of -1.
            try:
                min_ltime = branch_cache_min_ltimes[
                    tpc.project.source.connection.connection_name]
            except KeyError:
                min_ltime = -1
        else:
            min_ltime = -1
        branches = sorted(tpc.project.source.getProjectBranches(
            tpc.project, tenant, min_ltime))
        default_branch = tpc.project.source.getProjectDefaultBranch(
            tpc.project, tenant, min_ltime)
        if default_branch in branches:
            branches.remove(default_branch)
            branches = [default_branch] + branches
        static_branches = []
        always_dynamic_branches = []
        for b in branches:
            if tpc.includesBranch(b):
                static_branches.append(b)
            elif tpc.isAlwaysDynamicBranch(b):
                always_dynamic_branches.append(b)
        tpc.branches = static_branches
        tpc.dynamic_branches = always_dynamic_branches

        tpc.merge_modes = tpc.project.source.getProjectMergeModes(
            tpc.project, tenant, min_ltime)

    def _loadProjectKeys(self, connection_name, project):
        project.private_secrets_key, project.public_secrets_key = (
            self.keystorage.getProjectSecretsKeys(
                connection_name, project.name
            )
        )

        project.private_ssh_key, project.public_ssh_key = (
            self.keystorage.getProjectSSHKeys(connection_name, project.name)
        )

    def _loadTenantKeys(self, tenant):
        tenant.private_ssh_key, tenant.public_ssh_key = (
            self.keystorage.getTenantSSHKeys(tenant.name)
        )

    @staticmethod
    def _getProject(source, conf, current_include):
        extra_config_files = ()
        extra_config_dirs = ()

        if isinstance(conf, str):
            # Return a project object whether conf is a dict or a str
            project = source.getProject(conf)
            project_include = current_include
            shadow_projects = []
            project_exclude_unprotected_branches = None
            project_exclude_locked_branches = None
            project_include_branches = None
            project_exclude_branches = None
            project_always_dynamic_branches = None
            project_load_branch = None
            project_implied_branch_matchers = None
            project_configure_projects = None
            project_allow_reporter_jobs = None
        else:
            project_name = list(conf.keys())[0]
            project = source.getProject(project_name)
            shadow_projects = as_list(conf[project_name].get('shadow', []))

            # We check for None since the user may set include to an empty list
            if conf[project_name].get("include") is None:
                project_include = current_include
            else:
                project_include = frozenset(
                    as_list(conf[project_name]['include']))

            # If the user set this to true, add the extra classes;
            # ignore false.
            if conf[project_name].get("include-provider-config"):
                project_include = frozenset(
                    tuple(project_include) + TenantParser.provider_classes)

            project_exclude = frozenset(
                as_list(conf[project_name].get('exclude', [])))
            if project_exclude:
                project_include = frozenset(project_include - project_exclude)
            project_exclude_unprotected_branches = conf[project_name].get(
                'exclude-unprotected-branches', None)
            project_exclude_locked_branches = conf[project_name].get(
                'exclude-locked-branches', None)
            project_include_branches = conf[project_name].get(
                'include-branches', None)
            if project_include_branches is not None:
                project_include_branches = [
                    re.compile(b) for b in as_list(project_include_branches)
                ]
            exclude_branches = conf[project_name].get(
                'exclude-branches', None)
            if exclude_branches is not None:
                project_exclude_branches = [
                    re.compile(b) for b in as_list(exclude_branches)
                ]
            else:
                project_exclude_branches = None
            always_dynamic_branches = conf[project_name].get(
                'always-dynamic-branches', None)
            if always_dynamic_branches is not None:
                if project_exclude_branches is None:
                    project_exclude_branches = []
                    exclude_branches = []
                project_always_dynamic_branches = []
                for b in always_dynamic_branches:
                    rb = re.compile(b)
                    if b not in exclude_branches:
                        project_exclude_branches.append(rb)
                    project_always_dynamic_branches.append(rb)
            else:
                project_always_dynamic_branches = None
            if conf[project_name].get('extra-config-paths') is not None:
                extra_config_paths = as_list(
                    conf[project_name]['extra-config-paths'])
                extra_config_files = tuple([x for x in extra_config_paths
                                            if not x.endswith('/')])
                extra_config_dirs = tuple([x[:-1] for x in extra_config_paths
                                           if x.endswith('/')])
            project_load_branch = conf[project_name].get(
                'load-branch', None)
            project_implied_branch_matchers = conf[project_name].get(
                'implied-branch-matchers', None)
            configure_projects = as_list(conf[project_name].get(
                'configure-projects', None))
            if configure_projects is not None:
                project_configure_projects = []
                for p in configure_projects:
                    rp = re.compile(p)
                    project_configure_projects.append(rp)
            else:
                project_configure_projects = None
            project_allow_reporter_jobs = conf[project_name].get(
                'allow-reporter-jobs', None)

        tenant_project_config = model.TenantProjectConfig(project)
        tenant_project_config.load_classes = frozenset(project_include)
        tenant_project_config.shadow_projects = shadow_projects
        tenant_project_config.exclude_unprotected_branches = \
            project_exclude_unprotected_branches
        tenant_project_config.exclude_locked_branches = \
            project_exclude_locked_branches
        tenant_project_config.include_branches = project_include_branches
        tenant_project_config.exclude_branches = project_exclude_branches
        tenant_project_config.always_dynamic_branches = \
            project_always_dynamic_branches
        tenant_project_config.extra_config_files = extra_config_files
        tenant_project_config.extra_config_dirs = extra_config_dirs
        tenant_project_config.load_branch = project_load_branch
        tenant_project_config.implied_branch_matchers = \
            project_implied_branch_matchers
        tenant_project_config.configure_projects = \
            project_configure_projects
        tenant_project_config.allow_reporter_jobs = \
            project_allow_reporter_jobs
        return tenant_project_config

    def _getProjects(self, source, conf, current_include):
        # Return a project object whether conf is a dict or a str
        projects = []
        if isinstance(conf, str):
            # A simple project name string
            projects.append(self._getProject(source, conf, current_include))
        elif len(conf.keys()) > 1 and 'projects' in conf:
            # This is a project group
            if 'include' in conf:
                current_include = set(as_list(conf['include']))
            else:
                current_include = current_include.copy()

            # If the user set this to true, add the extra classes;
            # ignore false.
            if conf.get("include-provider-config"):
                current_include = frozenset(
                    tuple(current_include) + TenantParser.provider_classes)

            if 'exclude' in conf:
                exclude = set(as_list(conf['exclude']))
                current_include = current_include - exclude
            for project in conf['projects']:
                sub_projects = self._getProjects(
                    source, project, current_include)
                projects.extend(sub_projects)
        elif len(conf.keys()) == 1:
            # A project with overrides
            projects.append(self._getProject(
                source, conf, current_include))
        else:
            raise Exception("Unable to parse project %s", conf)
        return projects

    def loadTenantProjects(self, conf_tenant, executor):
        config_projects = []
        untrusted_projects = []

        # TODO: Add nodepool objects here (image, etc) when ready to
        # use zuul-launcher.
        untrusted_default_include = frozenset(self.job_classes)
        config_default_include = frozenset(self.all_classes)

        futures = []
        for source_name, conf_source in conf_tenant.get('source', {}).items():
            source = self.connections.getSource(source_name)

            for conf_repo in conf_source.get('config-projects', []):
                # tpcs = TenantProjectConfigs
                tpcs = self._getProjects(source, conf_repo,
                                         config_default_include)
                for tpc in tpcs:
                    tpc.trusted = True
                    tpc.allow_reporter_jobs = True
                    futures.append(executor.submit(
                        self._loadProjectKeys, source_name, tpc.project))
                    config_projects.append(tpc)

            for conf_repo in conf_source.get('untrusted-projects', []):
                tpcs = self._getProjects(source, conf_repo,
                                         untrusted_default_include)
                for tpc in tpcs:
                    tpc.trusted = False
                    futures.append(executor.submit(
                        self._loadProjectKeys, source_name, tpc.project))
                    untrusted_projects.append(tpc)

        for f in futures:
            f.result()
        for tpc in untrusted_projects:
            if tpc.configure_projects:
                for config_tpc in config_projects:
                    if tpc.canConfigureProject(
                            config_tpc, validation_only=True):
                        raise Exception(
                            f"Untrusted-project {tpc.project.name} may not "
                            f"configure "
                            f"config-project {config_tpc.project.name}")

        return config_projects, untrusted_projects

    def _cacheTenantYAML(self, abide, tenant, parse_context, min_ltimes,
                         executor, ignore_cat_exception=True):
        # min_ltimes can be the following: None (that means that we
        # should not use the file cache at all) or a nested dict of
        # project and branch to ltime.  A value of None usually means
        # we are being called from the command line config validator.

        # If min_ltimes is not None, then it is mutated and returned
        # with the actual ltimes of each entry in the unparsed branch
        # cache.

        # If the ltime is -1, then we should consider the file cache
        # valid.  If we have an unparsed branch cache entry for the
        # project-branch, we should use it, otherwise we should update
        # our unparsed branch cache from whatever is in the file
        # cache.

        # If the ltime is otherwise, then if our unparsed branch cache
        # is valid for that ltime, we should use the contents.
        # Otherwise if the files cache is valid for the ltime, we
        # should update our unparsed branch cache from the files cache
        # and use that.  Otherwise, we should run a cat job to update
        # the files cache, then update our unparsed branch cache from
        # that.

        # The circumstances under which this method is called are:

        # Prime:
        #   We'll either get an actual min_ltimes dict from the last
        #   reconfig, or -1 if this is a new tenant.
        #   In all cases, our unparsed branch cache will be empty, so
        #   we will always either load data from zk or issue a cat job
        #   as appropriate.

        # Background layout update:
        #   Otherwise, min_ltimes will always be the actual min_ltimes
        #   from the last reconfig.  No cat jobs should be needed; we
        #   either have an unparsed branch cache valid for the ltime,
        #   or we update it from ZK which should be valid.

        # Smart or full reconfigure:
        #   min_ltime is -1: a smart reconfig: consider the caches valid
        #   min_ltime is the event time: a full reconfig; we update
        #   both of the ccahes as necessary.

        # Tenant reconfigure:
        #   min_ltime is -1: this project-branch is unchanged by the
        #   tenant reconfig event, so consider the caches valid.
        #   min_ltime is the event time: this project-branch was updated
        #   so check the caches.

        jobs = []

        futures = []
        for tpc in tenant.all_tpcs:
            project = tpc.project
            # For each branch in the repo, get the zuul.yaml for that
            # branch.  Remember the branch and then implicitly add a
            # branch selector to each job there.  This makes the
            # in-repo configuration apply only to that branch.
            branches = tenant.getProjectBranches(project.canonical_name)
            for branch in branches:
                if not tpc.load_classes:
                    # If all config classes are excluded then do not
                    # request any getFiles jobs.
                    continue
                # In the submit call below, we dereference the
                # accumulator outside the threadpool since it won't be
                # able to get it once it starts.
                futures.append(executor.submit(self._cacheTenantYAMLBranch,
                                               abide, tenant,
                                               parse_context,
                                               parse_context.accumulator,
                                               min_ltimes, tpc, project,
                                               branch, jobs))
        for future in futures:
            future.result()

        for i, job in enumerate(jobs, start=1):
            try:
                try:
                    self._processCatJob(abide, tenant, parse_context, job,
                                        min_ltimes)
                except TimeoutError:
                    self.merger.cancel(job)
                    raise
            except Exception:
                self.log.exception("Error processing cat job")
                if not ignore_cat_exception:
                    # Cancel remaining jobs
                    for cancel_job in jobs[i:]:
                        self.log.debug("Canceling cat job %s", cancel_job)
                        try:
                            self.merger.cancel(cancel_job)
                        except Exception:
                            self.log.exception(
                                "Unable to cancel job %s", cancel_job)
                    raise

    def _cacheTenantYAMLBranch(
            self, abide, tenant, parse_context, error_accumulator,
            min_ltimes, tpc, project, branch, jobs):
        # We're inside of a threadpool, which means we have no
        # existing accumulator stack.  Start a new one.
        source_context = model.SourceContext(
            project.canonical_name, project.name,
            project.connection_name, branch, '')
        local_accumulator = error_accumulator.extend(
            source_context=source_context)
        parse_context.resetErrorContext(local_accumulator)
        # This is the middle section of _cacheTenantYAML, called for
        # each project-branch.  It's a separate method so we can
        # execute it in parallel.  The "jobs" argument is mutated and
        # accumulates a list of all merger jobs submitted.
        if min_ltimes is not None:
            config_object_cache = abide.getConfigObjectCache(
                project.canonical_name, branch)
            try:
                pb_ltime = min_ltimes[project.canonical_name][branch]
                # If our config object cache is valid for the time,
                # then we don't need to do anything else.
                coc_ltime = config_object_cache.getValidFor(
                    tpc, ZUUL_CONF_ROOT, pb_ltime)
                if coc_ltime is not None:
                    min_ltimes[project.canonical_name][branch] = coc_ltime
                    return
            except KeyError:
                self.log.exception(
                    "Min. ltime missing for project/branch")
                pb_ltime = -1

            files_cache = self.unparsed_config_cache.getFilesCache(
                project.canonical_name, branch)
            with self.unparsed_config_cache.readLock(
                    project.canonical_name):
                if files_cache.isValidFor(tpc, pb_ltime):
                    self.log.debug(
                        "Using files from cache for project "
                        "%s @%s: %s",
                        project.canonical_name, branch,
                        list(files_cache.keys()))
                    self._updateConfigObjectCache(
                        abide, tenant, source_context, files_cache,
                        parse_context, files_cache.ltime,
                        min_ltimes)
                    return

        extra_config_files = abide.getExtraConfigFiles(project.name)
        extra_config_dirs = abide.getExtraConfigDirs(project.name)
        if not self.merger:
            err = Exception(
                "Configuration files missing from cache. "
                "Check Zuul scheduler logs for more information.")
            parse_context.accumulator.addError(err)
            return
        ltime = self.zk_client.getCurrentLtime()
        job = self.merger.getFiles(
            project.source.connection.connection_name,
            project.name, branch,
            files=(['zuul.yaml', '.zuul.yaml'] +
                   list(extra_config_files)),
            dirs=['zuul.d', '.zuul.d'] + list(extra_config_dirs))
        self.log.debug("Submitting cat job %s for %s %s %s" % (
            job, project.source.connection.connection_name,
            project.name, branch))
        job.extra_config_files = extra_config_files
        job.extra_config_dirs = extra_config_dirs
        job.ltime = ltime
        job.source_context = source_context
        jobs.append(job)

    def _processCatJob(self, abide, tenant, parse_context, job, min_ltimes):
        # Called at the end of _cacheTenantYAML after all cat jobs
        # have been submitted
        self.log.debug("Waiting for cat job %s" % (job,))
        res = job.wait(self.merger.git_timeout)
        if not res:
            # We timed out
            raise TimeoutError(f"Cat job {job} timed out; consider setting "
                               "merger.git_timeout in zuul.conf")
        if not job.updated:
            raise Exception("Cat job %s failed" % (job,))
        self.log.debug("Cat job %s got files %s" %
                       (job, job.files.keys()))

        with parse_context.errorContext(source_context=job.source_context):
            self._updateConfigObjectCache(
                abide, tenant, job.source_context,
                job.files, parse_context,
                job.ltime, min_ltimes)

        # Save all config files in Zookeeper (not just for the current tpc)
        files_cache = self.unparsed_config_cache.getFilesCache(
            job.source_context.project_canonical_name,
            job.source_context.branch)
        with self.unparsed_config_cache.writeLock(
                job.source_context.project_canonical_name):
            # Prevent files cache ltime from going backward
            if files_cache.ltime >= job.ltime:
                self.log.info(
                    "Discarding job %s result since the files cache was "
                    "updated in the meantime", job)
                return
            # Since the cat job returns all required config files
            # for ALL tenants the project is a part of, we can
            # clear the whole cache and then populate it with the
            # updated content.
            files_cache.clear()
            for fn, content in job.files.items():
                # Cache file in Zookeeper
                if content is not None:
                    files_cache[fn] = content
            files_cache.setValidFor(job.extra_config_files,
                                    job.extra_config_dirs,
                                    job.ltime)

    def _updateConfigObjectCache(self, abide, tenant, source_context, files,
                                 parse_context, ltime, min_ltimes):
        # Read YAML from the file cache, parse it into objects, then
        # update the ConfigObjectCache.
        loaded = False
        tpc = tenant.project_configs[source_context.project_canonical_name]
        config_object_cache = abide.getConfigObjectCache(
            source_context.project_canonical_name,
            source_context.branch)
        valid_dirs = ("zuul.d", ".zuul.d") + tpc.extra_config_dirs
        for conf_root in (ZUUL_CONF_ROOT + tpc.extra_config_files
                          + tpc.extra_config_dirs):
            for fn in sorted(files.keys()):
                if not (fn == conf_root
                        or (conf_root in valid_dirs
                            and fn.startswith(f"{conf_root}/"))):
                    continue
                if not (file_data := files.get(fn)):
                    continue
                # Warn if there is more than one configuration in a
                # project-branch (unless an "extra" file/dir).  We
                # continue to add the data to the cache for use by
                # other tenants, but we will filter it out when we
                # retrieve it later.
                fn_root = fn.split('/')[0]
                if (fn_root in ZUUL_CONF_ROOT):
                    if (loaded and loaded != conf_root):
                        err = MultipleProjectConfigurations(source_context)
                        parse_context.accumulator.addError(err)
                    loaded = conf_root
                # Create a new source_context so we have unique filenames.
                source_context = source_context.copy()
                source_context.path = fn
                self.log.info(
                    "Loading configuration from %s" %
                    (source_context,))
                # Make a new error accumulator; we may be in a threadpool
                # so we can't use the stack.
                with parse_context.errorContext(source_context=source_context):
                    incdata = self.loadProjectYAML(
                        file_data, source_context, parse_context)
                config_object_cache.put(source_context.path, incdata, ltime)
        config_object_cache.setValidFor(tpc, ZUUL_CONF_ROOT, ltime)
        if min_ltimes is not None:
            min_ltimes[source_context.project_canonical_name][
                source_context.branch] = ltime

    def loadProjectYAML(self, data, source_context, parse_context):
        unparsed_config = model.UnparsedConfig()
        with parse_context.accumulator.catchErrors():
            r = safe_load_yaml(data, source_context)
            unparsed_config.extend(r)
        parsed_config = self.parseConfig(unparsed_config, parse_context)
        return parsed_config

    def _loadTenantParsedConfig(self, abide, tenant, parse_context):
        parsed_config = model.ParsedConfig()

        for project in tenant.config_projects:
            tpc = tenant.project_configs.get(project.canonical_name)
            branch = tpc.load_branch if tpc.load_branch else 'master'
            config_object_cache = abide.getConfigObjectCache(
                project.canonical_name,
                branch)
            branch_config = config_object_cache.get(tpc, ZUUL_CONF_ROOT)
            if branch_config:
                self.addProjectBranchConfig(
                    parse_context, parsed_config,
                    tenant, tpc, branch_config, trusted=True)

        for project in tenant.untrusted_projects:
            tpc = tenant.project_configs.get(project.canonical_name)
            branches = tenant.getProjectBranches(project.canonical_name)
            for branch in branches:
                config_object_cache = abide.getConfigObjectCache(
                    project.canonical_name,
                    branch)
                branch_config = config_object_cache.get(tpc, ZUUL_CONF_ROOT)
                if branch_config:
                    self.addProjectBranchConfig(
                        parse_context, parsed_config, tenant, tpc,
                        branch_config, trusted=False)

        return parsed_config

    def addProjectBranchConfig(self, parse_context, parsed_config,
                               tenant, tpc, branch_config, trusted):
        # Add items from branch_config to parsed_config as appropriate
        # for this tpc.
        classes = tpc.load_classes

        # It is not necessary to copy pragmas, but do copy the errors
        parsed_config.pragma_errors.extend(branch_config.pragma_errors)

        if 'pipeline' in classes:
            if not trusted and branch_config.pipelines:
                with parse_context.errorContext(
                        stanza='pipeline',
                        conf=branch_config.pipelines[0]):
                    parse_context.accumulator.addError(
                        PipelineNotPermittedError())
            else:
                parsed_config.pipeline_errors.extend(
                    branch_config.pipeline_errors)
                parsed_config.pipelines.extend(branch_config.pipelines)

        if 'job' in classes:
            parsed_config.job_errors.extend(
                branch_config.job_errors)
            parsed_config.jobs.extend(
                branch_config.jobs)
        if 'project-template' in classes:
            parsed_config.project_template_errors.extend(
                branch_config.project_template_errors)
            parsed_config.project_templates.extend(
                branch_config.project_templates)
        if 'project' in classes:
            parsed_config.project_errors.extend(
                branch_config.project_errors)
            parsed_config.projects.extend(
                branch_config.projects)
            for regex, projects in branch_config.projects_by_regex.items():
                parsed_config.projects_by_regex.setdefault(
                    regex, []).extend(projects)
        if 'nodeset' in classes:
            parsed_config.nodeset_errors.extend(
                branch_config.nodeset_errors)
            parsed_config.nodesets.extend(
                branch_config.nodesets)
        if 'secret' in classes:
            parsed_config.secret_errors.extend(
                branch_config.secret_errors)
            parsed_config.secrets.extend(
                branch_config.secrets)
        if 'semaphore' in classes:
            parsed_config.semaphore_errors.extend(
                branch_config.semaphore_errors)
            parsed_config.semaphores.extend(
                branch_config.semaphores)
        if 'queue' in classes:
            parsed_config.queue_errors.extend(
                branch_config.queue_errors)
            parsed_config.queues.extend(
                branch_config.queues)
        if 'image' in classes:
            parsed_config.image_errors.extend(
                branch_config.image_errors)
            parsed_config.images.extend(
                branch_config.images)
        if 'flavor' in classes:
            parsed_config.flavor_errors.extend(
                branch_config.flavor_errors)
            parsed_config.flavors.extend(
                branch_config.flavors)
        if 'label' in classes:
            parsed_config.label_errors.extend(
                branch_config.label_errors)
            parsed_config.labels.extend(
                branch_config.labels)
        if 'section' in classes:
            parsed_config.section_errors.extend(
                branch_config.section_errors)
            parsed_config.sections.extend(
                branch_config.sections)
        if 'provider' in classes:
            parsed_config.provider_errors.extend(
                branch_config.provider_errors)
            parsed_config.providers.extend(
                branch_config.providers)

    def parseConfig(self, unparsed_config, pcontext):
        parsed_config = model.ParsedConfig()

        # Handle pragma items first since they modify the source
        # context used by other classes.
        for config_pragma in unparsed_config.pragmas:
            with pcontext.errorContext(
                    error_list=parsed_config.pragma_errors,
                    stanza='pragma',
                    conf=config_pragma):
                with pcontext.accumulator.catchErrors():
                    pcontext.pragma_parser.fromYaml(config_pragma)

        for config_pipeline in unparsed_config.pipelines:
            with pcontext.errorContext(
                    error_list=parsed_config.pipeline_errors,
                    stanza='pipeline',
                    conf=config_pipeline):
                with pcontext.accumulator.catchErrors():
                    parsed_config.pipelines.append(
                        pcontext.pipeline_parser.fromYaml(config_pipeline))

        for config_image in unparsed_config.images:
            with pcontext.errorContext(
                    error_list=parsed_config.image_errors,
                    stanza='image',
                    conf=config_image):
                with pcontext.accumulator.catchErrors():
                    parsed_config.images.append(
                        pcontext.image_parser.fromYaml(config_image))

        for config_flavor in unparsed_config.flavors:
            with pcontext.errorContext(
                    error_list=parsed_config.flavor_errors,
                    stanza='flavor',
                    conf=config_flavor):
                with pcontext.accumulator.catchErrors():
                    parsed_config.flavors.append(
                        pcontext.flavor_parser.fromYaml(config_flavor))

        for config_label in unparsed_config.labels:
            with pcontext.errorContext(
                    error_list=parsed_config.label_errors,
                    stanza='label',
                    conf=config_label):
                with pcontext.accumulator.catchErrors():
                    parsed_config.labels.append(
                        pcontext.label_parser.fromYaml(config_label))

        for config_section in unparsed_config.sections:
            with pcontext.errorContext(
                    error_list=parsed_config.section_errors,
                    stanza='section',
                    conf=config_section):
                with pcontext.accumulator.catchErrors():
                    parsed_config.sections.append(
                        pcontext.section_parser.fromYaml(config_section))

        for config_provider in unparsed_config.providers:
            with pcontext.errorContext(
                    error_list=parsed_config.provider_errors,
                    stanza='provider',
                    conf=config_provider):
                with pcontext.accumulator.catchErrors():
                    parsed_config.providers.append(
                        pcontext.provider_parser.fromYaml(config_provider))

        for config_nodeset in unparsed_config.nodesets:
            with pcontext.errorContext(
                    error_list=parsed_config.nodeset_errors,
                    stanza='nodeset',
                    conf=config_nodeset):
                with pcontext.accumulator.catchErrors():
                    parsed_config.nodesets.append(
                        pcontext.nodeset_parser.fromYaml(config_nodeset))

        for config_secret in unparsed_config.secrets:
            with pcontext.errorContext(
                    error_list=parsed_config.secret_errors,
                    stanza='secret',
                    conf=config_secret):
                with pcontext.accumulator.catchErrors():
                    parsed_config.secrets.append(
                        pcontext.secret_parser.fromYaml(config_secret))

        for config_job in unparsed_config.jobs:
            with pcontext.errorContext(
                    error_list=parsed_config.job_errors,
                    stanza='job',
                    conf=config_job):
                with pcontext.accumulator.catchErrors():
                    parsed_config.jobs.append(
                        pcontext.job_parser.fromYaml(config_job))

        for config_semaphore in unparsed_config.semaphores:
            with pcontext.errorContext(
                    error_list=parsed_config.semaphore_errors,
                    stanza='semaphore',
                    conf=config_semaphore):
                with pcontext.accumulator.catchErrors():
                    parsed_config.semaphores.append(
                        pcontext.semaphore_parser.fromYaml(config_semaphore))

        for config_queue in unparsed_config.queues:
            with pcontext.errorContext(
                    error_list=parsed_config.queue_errors,
                    stanza='queue',
                    conf=config_queue):
                with pcontext.accumulator.catchErrors():
                    parsed_config.queues.append(
                        pcontext.queue_parser.fromYaml(config_queue))

        for config_template in unparsed_config.project_templates:
            with pcontext.errorContext(
                    error_list=parsed_config.project_template_errors,
                    stanza='project-template',
                    conf=config_template):
                with pcontext.accumulator.catchErrors():
                    parsed_config.project_templates.append(
                        pcontext.project_template_parser.fromYaml(
                            config_template))

        for config_project in unparsed_config.projects:
            with pcontext.errorContext(
                    error_list=parsed_config.project_errors,
                    stanza='project',
                    conf=config_project):
                with pcontext.accumulator.catchErrors():
                    # we need to separate the regex projects as they are
                    # processed differently later
                    name = config_project.get('name')
                    parsed_project = pcontext.project_parser.fromYaml(
                        config_project)
                    if name and name.startswith('^'):
                        parsed_config.projects_by_regex.setdefault(
                            name, []).append(parsed_project)
                    else:
                        parsed_config.projects.append(parsed_project)

        return parsed_config

    def _addLayoutItems(self, layout, tenant, parsed_config,
                        parse_context, dynamic_layout=False):
        # TODO(jeblair): make sure everything needing
        # reference_exceptions has it; add tests if needed.

        # In this method we are adding items to a layout, so the error
        # accumulator is the tenant error list, not the parsed_config
        # per-object error lists.
        for e in parsed_config.pragma_errors:
            layout.loading_errors.addError(e)

        if not dynamic_layout:
            for e in parsed_config.pipeline_errors:
                layout.loading_errors.addError(e)
            for pipeline in parsed_config.pipelines:
                with parse_context.errorContext(
                        stanza='pipeline', conf=pipeline):
                    with parse_context.accumulator.catchErrors():
                        manager = self.createManager(parse_context, pipeline,
                                                     tenant)
                        layout.addPipelineManager(manager)

        for e in parsed_config.nodeset_errors:
            layout.loading_errors.addError(e)
        for nodeset in parsed_config.nodesets:
            with parse_context.errorContext(stanza='nodeset', conf=nodeset):
                with parse_context.accumulator.catchErrors():
                    layout.addNodeSet(nodeset)

        for e in parsed_config.secret_errors:
            layout.loading_errors.addError(e)
        for secret in parsed_config.secrets:
            with parse_context.errorContext(stanza='secret', conf=secret):
                with parse_context.accumulator.catchErrors():
                    layout.addSecret(secret)

        for e in parsed_config.job_errors:
            layout.loading_errors.addError(e)
        for job in parsed_config.jobs:
            with parse_context.errorContext(stanza='job', conf=job):
                with parse_context.accumulator.catchErrors():
                    added = layout.addJob(job)
                    if not added:
                        self.log.debug(
                            "Skipped adding job %s which shadows "
                            "an existing job", job)

        for e in parsed_config.image_errors:
            layout.loading_errors.addError(e)
        for image in parsed_config.images:
            with parse_context.errorContext(stanza='image',
                                            conf=image):
                with parse_context.accumulator.catchErrors():
                    layout.addImage(image)
        for e in parsed_config.flavor_errors:
            layout.loading_errors.addError(e)
        for flavor in parsed_config.flavors:
            with parse_context.errorContext(stanza='flavor',
                                            conf=flavor):
                with parse_context.accumulator.catchErrors():
                    layout.addFlavor(flavor)
        for e in parsed_config.label_errors:
            layout.loading_errors.addError(e)
        for label in parsed_config.labels:
            with parse_context.errorContext(stanza='label',
                                            conf=label):
                with parse_context.accumulator.catchErrors():
                    layout.addLabel(label)
        for label in layout.labels.values():
            with parse_context.errorContext(stanza='label', conf=label):
                with parse_context.accumulator.catchErrors():
                    label.validateReferences(layout)

        if dynamic_layout:
            # We should not actually update the layout with new
            # semaphores, but so that we can validate that the config
            # is correct, create a shadow layout here to which we add
            # new semaphores so validation is complete.
            shadow_layout = model.Layout(tenant)
            shadow_layout.images = layout.images.copy()
            shadow_layout.flavors = layout.flavors.copy()
            shadow_layout.labels = layout.labels.copy()
        else:
            shadow_layout = layout
        for e in parsed_config.semaphore_errors:
            shadow_layout.loading_errors.addError(e)
        for semaphore in parsed_config.semaphores:
            with parse_context.errorContext(stanza='semaphore',
                                            conf=semaphore):
                with parse_context.accumulator.catchErrors():
                    shadow_layout.addSemaphore(semaphore)
        for e in parsed_config.section_errors:
            shadow_layout.loading_errors.addError(e)
        for section in parsed_config.sections:
            with parse_context.errorContext(stanza='section',
                                            conf=section):
                with parse_context.accumulator.catchErrors():
                    shadow_layout.addSection(section)
        for e in parsed_config.provider_errors:
            shadow_layout.loading_errors.addError(e)
        for provider_config in parsed_config.providers:
            with parse_context.errorContext(stanza='provider',
                                            conf=provider_config):
                with parse_context.accumulator.catchErrors():
                    shadow_layout.addProviderConfig(provider_config)

        # Add providers to the shadow (or real) layout
        for provider_config in shadow_layout.provider_configs.values():
            with parse_context.errorContext(stanza='provider',
                                            conf=provider_config):
                with parse_context.accumulator.catchErrors():
                    flat_config = provider_config.flattenConfig(
                        shadow_layout,
                        parse_context.connections.connections)
                    connection_name = flat_config.get('connection')
                    connection = parse_context.connections.connections.get(
                        connection_name)
                    if connection is None:
                        raise UnknownConnection(connection_name)
                    provider = connection.driver.getProvider(
                        self.zk_client, connection, tenant.name,
                        provider_config.canonical_name,
                        flat_config,
                        parse_context.system.system_id)
                    shadow_layout.addProvider(provider)

        # Now that all the objects are added, verify references to
        # other config objects.
        for nodeset in layout.nodesets.values():
            with parse_context.errorContext(stanza='nodeset', conf=nodeset):
                with parse_context.accumulator.catchErrors():
                    nodeset.validateReferences(layout)
        for jobs in layout.jobs.values():
            for job in jobs:
                with parse_context.errorContext(stanza='job', conf=job):
                    with parse_context.accumulator.catchErrors():
                        job.validateReferences(layout)
        for manager in layout.pipeline_managers.values():
            pipeline = manager.pipeline
            with parse_context.errorContext(stanza='pipeline', conf=pipeline):
                with parse_context.accumulator.catchErrors():
                    pipeline.validateReferences(layout)

        for e in parsed_config.queue_errors:
            layout.loading_errors.addError(e)
        for queue in parsed_config.queues:
            with parse_context.errorContext(stanza='queue', conf=queue):
                with parse_context.accumulator.catchErrors():
                    layout.addQueue(queue)

        for e in parsed_config.project_template_errors:
            layout.loading_errors.addError(e)
        for template in parsed_config.project_templates:
            with parse_context.errorContext(stanza='project-template',
                                            conf=template):
                with parse_context.accumulator.catchErrors():
                    layout.addProjectTemplate(template)

        for e in parsed_config.project_errors:
            layout.loading_errors.addError(e)
        for parsed_project in parsed_config.projects:
            with parse_context.errorContext(stanza='project',
                                            conf=parsed_project):
                with parse_context.accumulator.catchErrors():
                    # Attempt to resolve a possibly short project name
                    # in this tenant.
                    this_tpc = tenant.getTPC(parsed_project.name)
                    if this_tpc is None:
                        raise ProjectNotFoundError(parsed_project.name)
                    layout.addProjectConfig(this_tpc.project.canonical_name,
                                            parsed_project)

        # The project stanzas containing a regex are separated from
        # the normal project stanzas and organized by regex. We need
        # to loop over each regex and copy each stanza below the regex
        # for each matching project.  These should be added to the
        # layout after the normal projects because the first project
        # sets the default branch, merge mode, etc; and that should be
        # an explicit project if it exists.
        for regex, parsed_projects in parsed_config.projects_by_regex.items():
            projects_matching_regex = tenant.getProjectsByRegex(regex)

            for trusted, project in projects_matching_regex:
                for parsed_project in parsed_projects:
                    with parse_context.errorContext(stanza='project',
                                                    conf=parsed_project):
                        with parse_context.accumulator.catchErrors():
                            layout.addProjectConfig(project.canonical_name,
                                                    parsed_project)

        # Now that all the project pipelines are loaded, fixup and
        # verify references to other config objects.
        self._validateProjectPipelineConfigs(tenant, layout, parse_context)

    def _validateProjectPipelineConfigs(self, tenant, layout, parse_context):
        # Validate references to other config objects
        def inner_validate_ppcs(project_config, ppc):
            for jobs in ppc.job_list.jobs.values():
                for job in jobs:
                    # validate that the job exists on its own (an
                    # additional requirement for project-pipeline
                    # jobs)
                    layout.getJob(job.name)
                    job.validateReferences(
                        layout, project_config=project_config)

        for project_name in layout.project_configs:
            for project_config in layout.project_configs[project_name]:
                with parse_context.errorContext(stanza='project',
                                                conf=project_config):
                    with parse_context.accumulator.catchErrors():
                        for template_name in project_config.templates:
                            if template_name not in layout.project_templates:
                                raise TemplateNotFoundError(template_name)
                            project_templates = layout.getProjectTemplates(
                                template_name)
                            for p_tmpl in project_templates:
                                with parse_context.errorContext(
                                        stanza='project-template',
                                        conf=p_tmpl):
                                    acc = parse_context.accumulator
                                    with acc.catchErrors():
                                        for ppc in p_tmpl.pipelines.values():
                                            inner_validate_ppcs(
                                                project_config, ppc)
                        for ppc in project_config.pipelines.values():
                            inner_validate_ppcs(project_config, ppc)
            # Set a merge mode if we don't have one for this project.
            # This can happen if there are only regex project stanzas
            # but no specific project stanzas.
            _, project = tenant.getProject(project_name)
            project_metadata = layout.getProjectMetadata(project_name)
            tpc = tenant.project_configs[project.canonical_name]
            if project_metadata.merge_mode is None:
                mode = project.source.getProjectDefaultMergeMode(
                    project, valid_modes=tpc.merge_modes)
                project_metadata.merge_mode = model.MERGER_MAP[mode]
            if project_metadata.default_branch is None:
                default_branch = project.source.getProjectDefaultBranch(
                    project, tenant)
                project_metadata.default_branch = default_branch
            if tpc.merge_modes is not None:
                source_context = model.SourceContext(
                    project.canonical_name, project.name,
                    project.connection_name, None, None)
                with parse_context.errorContext(
                        source_context=source_context):
                    if project_metadata.merge_mode not in tpc.merge_modes:
                        mode = model.get_merge_mode_name(
                            project_metadata.merge_mode)
                        allowed_modes = list(map(model.get_merge_mode_name,
                                                 tpc.merge_modes))
                        err = Exception(f'Merge mode {mode} not supported '
                                        f'by project {project_name}. '
                                        f'Supported modes: {allowed_modes}.')
                        parse_context.accumulator.addError(err)

    def _parseLayout(self, tenant, data, parse_context, layout_uuid=None):
        # Don't call this method from dynamic reconfiguration because
        # it interacts with drivers and connections.
        layout = model.Layout(tenant, layout_uuid)
        self.log.debug("Created layout id %s", layout.uuid)
        for e in parse_context.error_list:
            layout.loading_errors.addError(e)
        parse_context.error_list.clear()
        self._addLayoutItems(layout, tenant, data, parse_context)
        for e in parse_context.error_list:
            layout.loading_errors.addError(e)
        return layout

    def createManager(self, parse_context, pipeline, tenant):
        if pipeline.manager_name == 'dependent':
            manager = zuul.manager.dependent.DependentPipelineManager(
                self.scheduler, pipeline, tenant)
        elif pipeline.manager_name == 'independent':
            manager = zuul.manager.independent.IndependentPipelineManager(
                self.scheduler, pipeline, tenant)
        elif pipeline.manager_name == 'serial':
            manager = zuul.manager.serial.SerialPipelineManager(
                self.scheduler, pipeline, tenant)
        elif pipeline.manager_name == 'supercedent':
            manager = zuul.manager.supercedent.SupercedentPipelineManager(
                self.scheduler, pipeline, tenant)
        return manager


class ConfigLoader(object):
    log = logging.getLogger("zuul.ConfigLoader")

    def __init__(self, connections, system, zk_client, zuul_globals,
                 unparsed_config_cache, statsd=None, scheduler=None,
                 merger=None, keystorage=None):
        self.connections = connections
        self.system = system
        self.zk_client = zk_client
        self.globals = zuul_globals
        self.scheduler = scheduler
        self.merger = merger
        self.keystorage = keystorage
        self.tenant_parser = TenantParser(
            connections, zk_client, scheduler, merger, keystorage,
            zuul_globals, statsd, unparsed_config_cache)
        self.authz_rule_parser = AuthorizationRuleParser()
        self.authz_role_parser = AuthorizationRoleParser()
        self.global_semaphore_parser = GlobalSemaphoreParser()
        self.api_root_parser = ApiRootParser()

    def expandConfigPath(self, config_path):
        if config_path:
            config_path = os.path.expanduser(config_path)
        if not os.path.exists(config_path):
            raise Exception("Unable to read tenant config file at %s" %
                            config_path)
        return config_path

    def readConfig(self, config_path, from_script=False,
                   tenants_to_validate=None):
        config_path = self.expandConfigPath(config_path)
        if not from_script:
            with open(config_path) as config_file:
                self.log.info("Loading configuration from %s" % (config_path,))
                data = yaml.safe_load(config_file)
        else:
            if not os.access(config_path, os.X_OK):
                self.log.error(
                    "Unable to read tenant configuration from a non "
                    "executable script (%s)" % config_path)
                data = []
            else:
                self.log.info(
                    "Loading configuration from script %s" % config_path)
                ret = subprocess.run(
                    [config_path], stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)
                try:
                    ret.check_returncode()
                    data = yaml.safe_load(ret.stdout)
                except subprocess.CalledProcessError as error:
                    self.log.error(
                        "Tenant config script exec failed: %s (%s)" % (
                            str(error), str(ret.stderr)))
                    data = []
        unparsed_abide = model.UnparsedAbideConfig()
        unparsed_abide.extend(data)

        available_tenants = list(unparsed_abide.tenants)
        tenants_to_validate = tenants_to_validate or available_tenants
        if not set(tenants_to_validate).issubset(available_tenants):
            invalid = tenants_to_validate.difference(available_tenants)
            raise RuntimeError(f"Invalid tenant(s) found: {invalid}")
        for tenant_name in tenants_to_validate:
            # Validate the voluptuous schema early when reading the config
            # as multiple subsequent steps need consistent yaml input.
            self.tenant_parser.getSchema()(unparsed_abide.tenants[tenant_name])
        return unparsed_abide

    def loadAuthzRules(self, abide, unparsed_abide):
        abide.authz_rules.clear()
        for conf_authz_rule in unparsed_abide.authz_rules:
            authz_rule = self.authz_rule_parser.fromYaml(conf_authz_rule)
            abide.authz_rules[authz_rule.name] = authz_rule
        abide.authz_roles.clear()
        for conf_authz_role in unparsed_abide.authz_roles:
            authz_role = self.authz_role_parser.fromYaml(conf_authz_role)
            abide.authz_roles[authz_role.name] = authz_role
        admin_role = model.AuthZAdminRole()
        abide.authz_roles['admin'] = admin_role
        read_role = model.AuthZReadRole()
        abide.authz_roles['read'] = read_role

    def loadSemaphores(self, abide, unparsed_abide):
        abide.semaphores.clear()
        for conf_semaphore in unparsed_abide.semaphores:
            semaphore = self.global_semaphore_parser.fromYaml(conf_semaphore)
            abide.semaphores[semaphore.name] = semaphore

    def loadTPCs(self, abide, unparsed_abide, tenants=None):
        # Load the global api root too
        if unparsed_abide.api_roots:
            api_root_conf = unparsed_abide.api_roots[0]
        else:
            api_root_conf = {}
        abide.api_root = self.api_root_parser.fromYaml(abide, api_root_conf)

        if tenants:
            tenants_to_load = {t: unparsed_abide.tenants[t] for t in tenants
                               if t in unparsed_abide.tenants}
        else:
            tenants_to_load = unparsed_abide.tenants

        # Pre-load TenantProjectConfigs so we can get and cache all of a
        # project's config files (incl. tenant specific extra config) at once.
        with ThreadPoolExecutor(max_workers=4) as executor:
            for tenant_name, unparsed_config in tenants_to_load.items():
                tpc_registry = model.TenantTPCRegistry()
                config_tpcs, untrusted_tpcs = (
                    self.tenant_parser.loadTenantProjects(unparsed_config,
                                                          executor)
                )
                for tpc in config_tpcs:
                    tpc_registry.addConfigTPC(tpc)
                for tpc in untrusted_tpcs:
                    tpc_registry.addUntrustedTPC(tpc)
                # This atomic replacement of TPCs means that we don't need to
                # lock the abide.
                abide.setTPCRegistry(tenant_name, tpc_registry)

    def loadTenant(self, abide, tenant_name, ansible_manager, unparsed_abide,
                   min_ltimes=None, layout_uuid=None,
                   branch_cache_min_ltimes=None, ignore_cat_exception=True):
        """(Re-)load a single tenant.

        Description of cache stages:

        We have a local unparsed branch cache on each scheduler and the
        global config cache in Zookeeper. Depending on the event that
        triggers (re-)loading of a tenant we must make sure that those
        caches are considered valid or invalid correctly.

        If provided, the ``min_ltimes`` argument is expected to be a
        nested dictionary with the project-branches. The value defines
        the minimum logical time that is required for a cached config to
        be considered valid::

            {
                "example.com/org/project": {
                    "master": 12234,
                    "stable": -1,
                },
                "example.com/common-config": {
                    "master": -1,
                },
                ...
            }

        There are four scenarios to consider when loading a tenant.

        1. Processing a tenant reconfig event:
           - The min. ltime for the changed project(-branches) will be
             set to the event's ``zuul_event_ltime`` (to establish a
             happened-before relation in respect to the config change).
             The min. ltime for all other project-branches will be -1.
           - Config for needed project-branch(es) is updated via cat job
             if the cache is not valid (cache ltime < min. ltime).
           - Cache in Zookeeper and local unparsed branch cache is
             updated. The ltime of the cache will be the timestamp
             created shortly before requesting the config via the
             mergers (only for outdated items).
        2. Processing a FULL reconfiguration event:
           - The min. ltime for all project-branches is given as the
             ``zuul_event_ltime`` of the reconfiguration event.
           - Config for needed project-branch(es) is updated via cat job
             if the cache is not valid (cache ltime < min. ltime).
             Otherwise the local unparsed branch cache or the global
             config cache in Zookeeper is used.
           - Cache in Zookeeper and local unparsed branch cache is
             updated, with the ltime shortly before requesting the
             config via the mergers (only for outdated items).
        3. Processing a SMART reconfiguration event:
           - The min. ltime for all project-branches is given as -1 in
             order to use cached data wherever possible.
           - Config for new project-branch(es) is updated via cat job if
             the project is not yet cached. Otherwise the local unparsed
             branch cache or the global config cache in Zookeper is
             used.
           - Cache in Zookeeper and local unparsed branch cache is
             updated, with the ltime shortly before requesting the
             config via the mergers (only for new items).
        4. (Re-)loading a tenant due to a changed layout (happens after
           an event according to one of the other scenarios was
           processed on another scheduler):
           - The min. ltime for all project-branches is given as -1 in
             order to only use cached config.
           - Local unparsed branch cache is updated if needed.

        """
        if tenant_name not in unparsed_abide.tenants:
            # Copy tenants dictionary to not break concurrent iterations.
            with abide.tenant_lock:
                tenants = abide.tenants.copy()
                del tenants[tenant_name]
                abide.tenants = tenants
            return None

        unparsed_config = unparsed_abide.tenants[tenant_name]
        with ThreadPoolExecutor(max_workers=4) as executor:
            new_tenant = self.tenant_parser.fromYaml(
                abide, unparsed_config, ansible_manager, executor, self.system,
                min_ltimes, layout_uuid, branch_cache_min_ltimes,
                ignore_cat_exception)
        # Copy tenants dictionary to not break concurrent iterations.
        with abide.tenant_lock:
            tenants = abide.tenants.copy()
            tenants[tenant_name] = new_tenant
            abide.tenants = tenants
        if len(new_tenant.layout.loading_errors):
            self.log.warning(
                "%s errors detected during %s tenant configuration loading",
                len(new_tenant.layout.loading_errors), tenant_name)
            # Log accumulated errors
            for err in new_tenant.layout.loading_errors.errors[:10]:
                self.log.warning(err.error)
        return new_tenant

    def _loadDynamicProjectData(self, abide, parsed_config, project,
                                files, additional_project_branches,
                                trusted, item, pcontext):
        log = get_annotated_logger(self.log, item.event)
        tenant = item.manager.tenant
        tpc = tenant.project_configs[project.canonical_name]
        if trusted:
            branches = [tpc.load_branch if tpc.load_branch else 'master']
        else:
            # Use the cached branch list; since this is a dynamic
            # reconfiguration there should not be any branch changes.
            branches = tenant.getProjectBranches(project.canonical_name,
                                                 include_always_dynamic=True)
            # Except that we might be dealing with a change on a
            # dynamic branch which hasn't shown up in our cached list
            # yet (since we don't reconfigure on dynamic branch
            # creation).  Add additional branches in the queue which
            # match the dynamic branch regexes.
            additional_branches = list(additional_project_branches.get(
                project.canonical_name, []))
            additional_branches = [b for b in additional_branches
                                   if b not in branches
                                   and tpc.isAlwaysDynamicBranch(b)]
            if additional_branches:
                branches = branches + additional_branches

        for branch in branches:
            fns1 = []
            fns2 = []
            fns3 = []
            fns4 = []
            files_entry = files and files.connections.get(
                project.source.connection.connection_name, {}).get(
                    project.name, {}).get(branch)
            # If there is no files entry at all for this
            # project-branch, then use the cached config.
            if files_entry is None:
                # Don't use the cached config for a dynamic branch
                if tpc.isAlwaysDynamicBranch(branch):
                    continue
                config_object_cache = abide.getConfigObjectCache(
                    project.canonical_name, branch)
                branch_config = config_object_cache.get(tpc, ZUUL_CONF_ROOT)
                if branch_config:
                    self.tenant_parser.addProjectBranchConfig(
                        pcontext, parsed_config,
                        tenant, tpc, branch_config, trusted=trusted)
                continue
            # Otherwise, do not use the cached config (even if the
            # files are empty as that likely means they were deleted).
            files_list = files_entry.keys()
            for fn in files_list:
                if fn.startswith("zuul.d/"):
                    fns1.append(fn)
                if fn.startswith(".zuul.d/"):
                    fns2.append(fn)
                for ef in tpc.extra_config_files:
                    if fn == ef:
                        fns3.append(fn)
                for ed in tpc.extra_config_dirs:
                    if fn.startswith(ed + '/'):
                        fns4.append(fn)
            fns = (["zuul.yaml"] + sorted(fns1) + [".zuul.yaml"] +
                   sorted(fns2) + fns3 + sorted(fns4))
            loaded = None
            for fn in fns:
                data = files.getFile(project.source.connection.connection_name,
                                     project.name, branch, fn)
                if data:
                    source_context = model.SourceContext(
                        project.canonical_name, project.name,
                        project.connection_name, branch, fn)
                    with pcontext.errorContext(source_context=source_context):
                        # Prevent mixing configuration source
                        conf_root = fn.split('/')[0]

                        # Don't load from more than one configuration in a
                        # project-branch (unless an "extra" file/dir).
                        if (conf_root in ZUUL_CONF_ROOT):
                            if loaded and loaded != conf_root:
                                log.warning(
                                    "Configuration in %s ignored because "
                                    "project-branch is already configured",
                                    source_context)
                                item.warning(
                                    "Configuration in %s ignored because "
                                    "project-branch is already configured" %
                                    source_context)
                                continue
                            loaded = conf_root

                        log.info(
                            "Loading configuration dynamically from %s" %
                            (source_context,))
                        branch_config = self.tenant_parser.loadProjectYAML(
                            data, source_context, pcontext)

                        self.tenant_parser.addProjectBranchConfig(
                            pcontext, parsed_config,
                            tenant, tpc, branch_config, trusted=trusted)

    def createDynamicLayout(self, item, files,
                            additional_project_branches,
                            ansible_manager,
                            include_config_projects=False):
        abide = self.scheduler.abide
        tenant = item.manager.tenant
        event_id = item.event
        log = get_annotated_logger(self.log, event_id)
        pcontext = ParseContext(self.connections, self.globals,
                                self.system, ansible_manager)
        config = model.ParsedConfig()

        if include_config_projects:
            include_files = files
        else:
            # If we are not performing dynamic inclusion on
            # config_projects, then pass an empty files object in so
            # it is not used and all configuration comes from the
            # cache.
            include_files = None

        for project in tenant.config_projects:
            self._loadDynamicProjectData(abide, config, project, include_files,
                                         additional_project_branches,
                                         True, item, pcontext)
        for project in tenant.untrusted_projects:
            self._loadDynamicProjectData(abide, config, project, files,
                                         additional_project_branches,
                                         False, item, pcontext)

        layout = model.Layout(tenant, item.layout_uuid)
        log.debug("Created layout id %s", layout.uuid)
        for e in pcontext.error_list:
            layout.loading_errors.addError(e)
        pcontext.error_list.clear()
        if not include_config_projects:
            # NOTE: the actual pipeline objects (complete with queues
            # and enqueued items) are copied by reference here.  This
            # allows our shadow dynamic configuration to continue to
            # interact with all the other changes, each of which may
            # have their own version of reality.  We do not support
            # creating, updating, or deleting pipelines in dynamic
            # layout changes.
            layout.pipeline_managers = tenant.layout.pipeline_managers

            # NOTE: the semaphore definitions are copied from the
            # static layout here. For semaphores there should be no
            # per patch max value but exactly one value at any
            # time. So we do not support dynamic semaphore
            # configuration changes.
            layout.semaphores = tenant.layout.semaphores
            # We also don't support dynamic changes to
            # provider-related objects.
            layout.sections = tenant.layout.sections
            layout.providers = tenant.layout.providers
            dynamic_layout = True
        else:
            dynamic_layout = False

        self.tenant_parser._addLayoutItems(layout, tenant, config,
                                           pcontext,
                                           dynamic_layout=dynamic_layout)
        for e in pcontext.error_list:
            layout.loading_errors.addError(e)
        return layout
