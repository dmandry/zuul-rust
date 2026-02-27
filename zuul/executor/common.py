# Copyright 2018 SUSE Linux GmbH
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

import copy
import os

from zuul.lib import strings
from zuul.zk.components import COMPONENT_REGISTRY


def make_src_dir(canonical_hostname, name, scheme):
    return os.path.join('src',
                        strings.workspace_project_path(
                            canonical_hostname,
                            name,
                            scheme))


def make_change(item, change):
    ret = change.toDict()
    if sha := item.merged_change_shas.get(change.cache_key):
        ret['merge_commit_id'] = sha
    return ret


def construct_build_params(uuid, connections, job, item, pipeline,
                           dependent_changes=[], merger_items=[],
                           redact_secrets_and_keys=True):
    """Returns a list of all the parameters needed to build a job.

    These parameters may be passed to zuul-executors (via ZK) to perform
    the job itself.

    Alternatively they contain enough information to load into another build
    environment - for example, a local runner.
    """
    tenant = item.manager.tenant
    change = item.getChangeForJob(job)
    project = dict(
        name=change.project.name,
        short_name=change.project.name.split('/')[-1],
        canonical_hostname=change.project.canonical_hostname,
        canonical_name=change.project.canonical_name,
        src_dir=make_src_dir(change.project.canonical_hostname,
                             change.project.name,
                             job.workspace_scheme),
    )

    # We override some values like project, so set the change fields
    # first.
    zuul_params = change.toDict()
    zuul_params.update(dict(
        build=uuid,
        buildset=item.current_build_set.uuid,
        ref=change.ref,
        buildset_refs=[make_change(item, c) for c in item.changes],
        build_refs=[make_change(item, c) for c in item.changes
                    if c.cache_key in job.all_refs],
        pipeline=pipeline.name,
        post_review=pipeline.post_review,
        job=job.name,
        project=project,
        tenant=tenant.name,
        event_id=item.event.zuul_event_id if item.event else None,
        jobtags=sorted(job.tags),
        include_vars=job.include_vars,
    ))
    if hasattr(change, 'message'):
        zuul_params['message'] = strings.b64encode(change.message)
    zuul_params['projects'] = {}  # Set below

    # Fixup the src_dir for the items based on this job
    dependent_changes = copy.deepcopy(dependent_changes)
    for dep_change in dependent_changes:
        dep_change['project']['src_dir'] = make_src_dir(
            dep_change['project']['canonical_hostname'],
            dep_change['project']['name'],
            job.workspace_scheme)
    # Fixup the src_dir for the refs based on this job
    for r in zuul_params['buildset_refs']:
        r['project']['src_dir'] = make_src_dir(
            r['project']['canonical_hostname'],
            r['project']['name'],
            job.workspace_scheme)
        # TODO: backwards compat, remove after zuul 14
        r['src_dir'] = make_src_dir(
            r['project']['canonical_hostname'],
            r['project']['name'],
            job.workspace_scheme)
    for r in zuul_params['build_refs']:
        r['project']['src_dir'] = make_src_dir(
            r['project']['canonical_hostname'],
            r['project']['name'],
            job.workspace_scheme)
        # TODO: backwards compat, remove after zuul 14
        r['src_dir'] = make_src_dir(
            r['project']['canonical_hostname'],
            r['project']['name'],
            job.workspace_scheme)

    zuul_params['items'] = dependent_changes
    zuul_params['child_jobs'] = [
        x.name for x in item.current_build_set.job_graph.
        getDirectDependentJobs(job)
    ]

    params = dict()
    (
        params["parent_data"],
        params["secret_parent_data"],
        artifact_data
    ) = item.getJobParentData(job)
    if artifact_data:
        zuul_params['artifacts'] = artifact_data

    params['job_ref'] = job.getPath()
    params['items'] = merger_items
    params['projects'] = []
    if hasattr(change, 'branch'):
        params['branch'] = change.branch
    else:
        params['branch'] = None
    params['repo_state_keys'] = item.current_build_set.repo_state_keys

    params['ssh_keys'] = []
    if COMPONENT_REGISTRY.model_api >= 36:
        if redact_secrets_and_keys:
            params['ssh_keys'].append("REDACTED")
        else:
            params['ssh_keys'].append(
                dict(tenant_name=tenant.name)
            )
    if pipeline.post_review:
        if redact_secrets_and_keys:
            params['ssh_keys'].append("REDACTED")
        else:
            params['ssh_keys'].append(dict(
                connection_name=change.project.connection_name,
                project_name=change.project.name))
    params['pipeline_builds_images'] = pipeline.builds_images
    params['zuul'] = zuul_params
    projects = set()
    required_projects = set()

    def make_project_dict(project, override_branch=None,
                          override_checkout=None):
        project_metadata = item.current_build_set.job_graph.\
            getProjectMetadata(project.canonical_name)
        if project_metadata:
            project_default_branch = project_metadata.default_branch
        else:
            project_default_branch = 'master'
        connection = project.source.connection
        return dict(connection=connection.connection_name,
                    name=project.name,
                    canonical_name=project.canonical_name,
                    override_branch=override_branch,
                    override_checkout=override_checkout,
                    default_branch=project_default_branch)

    if job.required_projects:
        for job_project in job.required_projects.values():
            (trusted, project) = tenant.getProject(
                job_project.project_name)
            if project is None:
                raise Exception("Unknown project %s" %
                                (job_project.project_name,))
            params['projects'].append(
                make_project_dict(project,
                                  job_project.override_branch,
                                  job_project.override_checkout))
            projects.add(project)
            required_projects.add(project)

    if job.include_vars:
        for iv in job.include_vars:
            source = connections.getSource(iv['connection'])
            project = source.getProject(iv['project'])
            if project not in projects:
                params['projects'].append(make_project_dict(project))
                projects.add(project)

    for pb in job.all_playbooks:
        source = connections.getSource(pb['connection'])
        project = source.getProject(pb['project'])
        if project not in projects:
            params['projects'].append(make_project_dict(project))
            projects.add(project)
        for role in pb['roles']:
            source = connections.getSource(role['connection'])
            project = source.getProject(role['project'])
            if project not in projects:
                params['projects'].append(make_project_dict(project))
                projects.add(project)

    for dep_change in dependent_changes:
        try:
            (_, project) = item.manager.tenant.getProject(
                dep_change['project']['canonical_name'])
            if not project:
                raise KeyError()
        except Exception:
            # We have to find the project this way because it may not
            # be registered in the tenant (ie, a foreign project).
            source = connections.getSourceByCanonicalHostname(
                dep_change['project']['canonical_hostname'])
            project = source.getProject(dep_change['project']['name'])

        if not job.includesProject(project, change, item):
            continue
        if project not in projects:
            params['projects'].append(make_project_dict(project))
            projects.add(project)
    for p in projects:
        zuul_params['projects'][p.canonical_name] = (dict(
            name=p.name,
            short_name=p.name.split('/')[-1],
            # Duplicate this into the dict too, so that iterating
            # project.values() is easier for callers
            canonical_name=p.canonical_name,
            canonical_hostname=p.canonical_hostname,
            src_dir=make_src_dir(
                p.canonical_hostname,
                p.name,
                job.workspace_scheme),
            required=(p in required_projects),
        ))

    if item.event:
        params['zuul_event_id'] = item.event.zuul_event_id
    return params


def zuul_params_from_job(job):
    zuul_params = {
        "job": job.name,
        "voting": job.voting,
        "pre_timeout": job.pre_timeout,
        "timeout": job.timeout,
        "post_timeout": job.post_timeout,
        "jobtags": sorted(job.tags),
        "_inheritance_path": list(job.inheritance_path),
    }
    if job.artifact_data:
        zuul_params['artifacts'] = job.artifact_data
    if job.override_checkout:
        zuul_params['override_checkout'] = job.override_checkout
    return zuul_params
