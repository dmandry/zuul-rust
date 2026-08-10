# Copyright 2025 Acme Gating, LLC
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
import logging


class UploadPlanner:
    def __init__(self, launcher, image_build_artifact,
                 pending_uploads, all_uploads):
        self.log = logging.getLogger('zuul.UploadPlanner')
        self.launcher = launcher
        self.image_build_artifact = image_build_artifact
        # All pending uploads for this artifact.  The uploads may have
        # different configurations.
        self.pending_uploads = pending_uploads
        # All uploads for this artifact, even completed ones
        self.all_uploads = all_uploads
        # Group uploads by config hash
        self.uploads_by_hash = collections.defaultdict(list)
        for upload in self.all_uploads:
            self.uploads_by_hash[upload.config_hash].append(upload)

        self.jobs = dict()  # upload.uuid -> job
        self.import_jobs = []
        self.copy_jobs = []
        self.upload_jobs = []
        self.upload_args = {}  # upload.uuid -> args

    def plan(self):
        self._getUploadArguments()
        pending_uploads = self.pending_uploads[:]
        self._handleImports(pending_uploads)
        self._handleCopiesAndUploads(pending_uploads)

    def _getUploadArguments(self):
        for upload in self.pending_uploads:
            # The upload has a list of providers with identical
            # configurations.  Pick one of them as a representative.
            provider_cname = upload.providers[0]
            provider = self.launcher._getProviderByCanonicalName(
                provider_cname)
            provider_image = None
            for image in provider.images.values():
                if image.canonical_name == upload.canonical_name:
                    provider_image = image
            if provider_image is None:
                raise Exception(
                    f"Unable to find image {upload.canonical_name}")

            metadata = provider.getUploadTags(
                self.launcher.system.system_id,
                provider_image,
                upload.uuid,
            )
            artifact = self.image_build_artifact
            image_name = f'{provider_image.name}-{upload.uuid}'

            self.upload_args[upload.uuid] = dict(
                provider=provider,
                provider_image=provider_image,
                image_name=image_name,
                url=artifact.url,
                image_format=artifact.format,
                metadata=metadata,
                md5=artifact.md5sum,
                sha256=artifact.sha256,
            )

    def _handleImports(self, pending_uploads):
        for upload in pending_uploads[:]:
            args = self.upload_args[upload.uuid]
            if 'import' not in args['provider_image'].upload_methods:
                continue
            job = args['provider'].getImageImportJob(
                args['url'],
                args['provider_image'],
                args['image_name'],
                args['image_format'],
                args['metadata'],
                args['md5'],
                args['sha256'],
            )
            if job:
                pending_uploads.remove(upload)
                self.import_jobs.append((upload, job))
                self.jobs[upload.uuid] = job

    def _handleCopiesAndUploads(self, pending_uploads):
        # Copies are grouped by hash since we copy the config as well
        # as the artifact data
        for config_hash, hash_uploads in self.uploads_by_hash.items():
            self._handleCopiesInner(config_hash, hash_uploads,
                                    pending_uploads)

    def _handleCopiesInner(self, config_hash, hash_uploads, pending_uploads):
        # Handle copies for a single image config hash

        # This creates a dictionary whose keys are potential copy
        # sources (they come from all the uploads, finished or not),
        # and whose values are jobs representing uploads that could
        # potentially copy from them.
        source_uploads = {}
        pending_hash_uploads = [u for u in pending_uploads
                                if u.config_hash == config_hash]
        for upload in pending_hash_uploads:
            for source_upload in hash_uploads:
                # Make sure we at least have any empty list for every
                # source upload (we'll use that below).
                source_upload_targets = source_uploads.setdefault(
                    source_upload, [])
                if source_upload is upload:
                    continue
                args = self.upload_args[upload.uuid]
                if 'copy' not in args['provider_image'].upload_methods:
                    continue
                source_provider_cname = source_upload.providers[0]
                source_provider = self.launcher._getProviderByCanonicalName(
                    source_provider_cname)
                job = args['provider'].getImageCopyJob(
                    source_provider,
                    args['provider_image'],
                    args['image_name'],
                    args['image_format'],
                    args['metadata'],
                    args['md5'],
                    args['sha256'],
                )
                if job:
                    source_upload_targets.append((upload, job))

        # For every source upload that has completed, we are ready to
        # run any copy job that can copy from it.
        for source_upload, jobs in source_uploads.items():
            if not source_upload.external_id:
                continue
            for (upload, job) in jobs:
                if upload not in pending_hash_uploads:
                    continue
                # These jobs have no dependencies, so we add them to copy_jobs.
                self.copy_jobs.append((upload, job, source_upload.external_id))
                self.jobs[upload.uuid] = job
                pending_hash_uploads.remove(upload)
                pending_uploads.remove(upload)

        # For any remaining potential copy job, we're going to need to
        # wait for either an import job or an upload job.

        # We sort by how many copy jobs an upload can serve, in order
        # to try to serve the most copies with the fewest uploads
        # (though typically we expect any single copy to serve all the
        # uploads for a given provider).
        for source_upload, jobs in sorted(source_uploads.items(),
                                          key=lambda x: len(x[1]),
                                          reverse=True):
            if source_upload.external_id:
                continue
            # From this point, we need to be able to wait on a job, so
            # only look at pending uploads.
            if source_upload not in self.pending_uploads:
                continue
            # See if we have something we can wait on (presumably an
            # import job)
            source_job = self.jobs.get(source_upload.uuid)
            if not source_job:
                # We need an upload job for this
                args = self.upload_args[source_upload.uuid]
                if 'upload' not in args['provider_image'].upload_methods:
                    continue
                source_job = args['provider'].getImageUploadJob(
                    args['provider_image'],
                    args['image_name'],
                    args['image_format'],
                    args['metadata'],
                    args['md5'],
                    args['sha256'],
                )
                if source_job:
                    self.upload_jobs.append((source_upload, source_job))
                    self.jobs[source_upload.uuid] = source_job
                    pending_hash_uploads.remove(source_upload)
                    pending_uploads.remove(source_upload)
                else:
                    # Apparently we can't create an upload job
                    continue

            for (upload, job) in jobs:
                if upload not in pending_hash_uploads:
                    continue
                # These jobs can only be run after their dependencies
                # finish, so we don't add to copy_jobs, but we do add
                # to the source_job dependents.
                source_job.dependents.append((upload, job))
                self.jobs[upload.uuid] = job
                pending_hash_uploads.remove(upload)
                pending_uploads.remove(upload)
