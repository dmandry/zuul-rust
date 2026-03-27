# Copyright 2024 Acme Gating, LLC
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

import logging

from zuul.lib.result_data import get_artifacts_from_result_data
from zuul.reporter import BaseReporter


class ZuulReporter(BaseReporter):
    def __init__(self, driver, connection, pipeline, config):
        super().__init__(driver, connection, config)
        self.log = logging.getLogger('zuul.reporter')
        self.pipeline = pipeline
        self.image_built = config.get('image-built', False)
        self.image_validated = config.get('image-validated', None)

    def report(self, item, phase1=True, phase2=True):
        if not phase2:
            return
        if self.image_built:
            self.reportImageBuilt(item)
        elif self.image_validated is not None:
            self.reportImageValidated(item)

    def reportImageBuilt(self, item):
        sched = self.driver.sched

        for build in item.current_build_set.getBuilds():
            if not build.job.image_build_name:
                continue
            image_name = build.job.image_build_name
            # Allow the use of an image in a dynamic layout
            layout = item.manager.getLayout(item)
            image = layout.images[image_name]
            ibas = []
            for artifact in get_artifacts_from_result_data(
                    build.result_data,
                    logger=self.log):
                if metadata := artifact.get('metadata'):
                    if metadata.get('type') == 'zuul_image':
                        iba = sched.createImageBuildArtifact(
                            item.manager.tenant.name, image, build,
                            metadata, artifact['url'],
                            self.image_validated)
                        ibas.append(iba)
            for iba in ibas:
                sched.createImageUploads(iba)

    def reportImageValidated(self, item):
        sched = self.driver.sched

        for build in item.current_build_set.getBuilds():
            if not build.job.image_build_name:
                continue
            sched.validateImageUpload(
                item.event.image_upload_uuid,
                build,
                self.image_validated,
            )


def getSchema():
    zuul_reporter = {
        'image-built': bool,
        'image-validated': bool,
    }

    return zuul_reporter
