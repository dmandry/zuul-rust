# Copyright 2024-2026 Acme Gating, LLC
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

# This file contains provider-related schema chunks that can be reused
# by multiple drivers.  When adding new configuration options, if they
# can be used by more than one driver, add them here instead of in the
# driver.

import voluptuous as vs
from zuul.lib.voluputil import (
    Constant,
    Nullable,
    Optional,
    Required,
    assemble,
)

# Labels

# The label attributes which can appear either in the main body of the
# section stanza, or in a section/provider label, or in a standalone
# label.
common_label = vs.Schema({
    Optional(
        'boot-timeout', default=300,
        doc="""The time (in seconds) to wait for a node to boot.""",
    ): int,
    Optional(
        'max-ready-age', default=0,
        doc="""\
        The time (in seconds) an unassigned node should stay in ready state.
        """
    ): int,
    Optional(
        'max-age', default=0,
        doc="""\
        The time (in seconds) since creation that a node may be
        available for use.  Ready nodes older than this time will be
        deleted.
        """
    ): int,
    Optional(
        'min-retention-time', default=0,
        doc="""\
        The time (in seconds) since an instance was launched, during
        which a node will not be deleted. For node resources with
        minimum billing times, this can be used to ensure that the
        instance is retained for at least the minimum billing interval.

        This setting takes precedence over `max-[ready-]age`.
        """
    ): int,
    Optional(
        'snapshot-timeout', default=3600,
        doc="""The time (in seconds) to wait for a snapshot to complete."""
    ): int,
    Optional(
        'snapshot-expiration', default=3600 * 24 * 7,
        doc="""The time (in seconds) until a snapshot expires."""
    ): int,
    Optional(
        'slots', default=1,
        doc="""\
        How many jobs are permitted run on the same node simultaneously."""
    ): int,
    Optional(
        'reuse', default=False,
        doc="""\
        Should the node be reused (True) or deleted (False) after use."""
    ): bool,
    Optional(
        'executor-zone',
        doc="""\
        Specify that a Zuul executor in the specified zone is
        used to run jobs with nodes from this label.
        """,
    ): Nullable(str),
    Optional(
        'tags',
        doc="""\
        A dictionary of tags to add to nodes.  Avoid the use of
        `zuul_` as a key prefix since Zuul uses this for internal
        values.
        """,
        default=dict): {str: str},
    Optional(
        'final',
        doc="""\
        Whether the configuration of the label may be updated
        by values in label-defaults or overidden with a new definition
        by sections or providers lower in the hierarchy than the point
        at which the final attribute is applied.""",
        default=False): vs.Any(
            Constant(True,
                     doc="The label may not be updated or overidden."),
            Constant(False,
                     doc="The label may be updated or overidden."),
            Constant('allow-override',
                     doc="""\
                     The label may not be updated by label-defaults
                     but may be explicitly overidden by redefining
                     it in a new 'label' entry.""")),
})

# The label attributes that can appear in a section/provider label or
# a standalone label (but not in the section body).
base_label = vs.Schema({
    Required(
        'name',
        doc="""\
        The name of the label.  Used to refer to the label in Zuul
        configuration.""",
    ): str,
    Optional(
        'description',
        doc="""\
        A textual description of the label for reference purposes.""",
    ): Nullable(str),
    Optional(
        'image',
        doc="""\
        The image to use with this label.
        """,
    ): Nullable(str),
    Optional(
        'flavor',
        doc="""\
        The flavor to use with this label.
        """,
    ): Nullable(str),
})

internal_base_label = vs.Schema({
    Required('project_canonical_name'): str,
    Required('config_hash'): vs.Any(None, str),
    Optional('min-ready', default=None): vs.Any(None, int),
    Optional('max-nodes', default=None): vs.Any(None, int),
})

# Azure doesn't take a key-name, so this is separate.
host_key_checking = vs.Schema({
    Optional(
        'host-key-checking',
        doc="""\
        Whether to validate SSH host keys.  When true, this helps
        ensure that nodes are ready to receive SSH connections before
        they are used.  When set to false, Zuul will not attempt to
        ssh-keyscan nodes after they are booted.  Disable this if the
        zuul-launcher and the nodes it launches are on different
        networks, where the launcher is unable to reach the nodes
        directly.""",
        default=True,
    ): bool,
})

# Label attributes that are common to any kind of ssh-based driver.
ssh_label = assemble(
    vs.Schema({
        Optional(
            'key-name',
            doc="""\
            The name of a keypair that will be used when
            booting the node.
            """,
        ): Nullable(str),
    }),
    host_key_checking,
)

# Images

# The image attributes which can appear either in the main body of the
# section stanza, or in a section/provider image, or in a standalone
# image.
common_image = vs.Schema({
    Optional(
        'username',
        config=False,
        doc="""\
        The username Zuul should use when connecting to the node.
        """,
    ): Nullable(str),
    Optional(
        'connection-type',
        config=False,
        doc="""\
        The connection type that a consumer should use when connecting
        to the node.
        """,
    ): Nullable(vs.Any(
        Constant('winrm',
                 doc="A winrm connection."),
        Constant('ssh',
                 doc="An ssh connection."),
    )),
    Optional(
        'connection-port',
        config=False,
        doc="""\
        The port that Zuul should use when connecting to the node.
        For most nodes this is not necessary. This defaults to 22 when
        ``connection-type`` is 'ssh' and 5986 when it is 'winrm'.""",
    ): Nullable(int),
    Optional(
        'python-path',
        config=False,
        doc="""\
        The path of the default python interpreter.  Used by Zuul to set
        ``ansible_python_interpreter``.  The special value ``auto`` will
        direct Zuul to use inbuilt Ansible logic to select the
        interpreter.""",
    ): Nullable(str),
    Optional(
        'shell-type',
        config=False,
        doc="""\
        The shell type of the node's default shell executable. Used by Zuul
        to set ``ansible_shell_type``. This setting should only be used

        - For a windows image with the experimental `connection-type` ``ssh``
          in which case ``cmd`` or ``powershell`` should be set
          and reflect the node's ``DefaultShell`` configuration.
        - If the default shell is not Bourne compatible (sh), but instead
          e.g. ``csh`` or ``fish``, and the user is aware that there is a
          long-standing issue with ``ansible_shell_type`` in combination
          with ``become``.
        """,
    ): Nullable(str),
    Optional(
        'import-timeout',
        config=False,
        doc="""\
        The limit on the amount of time a successful image import can
        take.""",
        default=300): int,
    Optional(
        'final',
        config=False,
        doc="""\
        Whether the configuration of the label may be updated
        by values in label-defaults or overidden with a new definition
        by sections or providers lower in the hierarchy than the point
        at which the final attribute is applied.""",
        default=False): vs.Any(
            Constant(True,
                     doc="The label may not be updated or overidden."),
            Constant(False,
                     doc="The label may be updated or overidden."),
            Constant('allow-override',
                     doc="""\
                     The label may not be updated by label-defaults
                     but may be explicitly overidden by redefining
                     it in a new 'label' entry.""")),
})

# Same as above, but only for cloud drivers.
cloud_image = vs.Schema({
    Optional(
        'userdata',
        doc="""\
        A string of userdata for a node.  Systems such as "cloud-init"
        may use this to configure the node on boot.
        """,
    ): Nullable(str),
})

# Same as above, but only for zuul images.
common_image_zuul = vs.Schema({
    Optional(
        'upload-methods',
        config=False,
        doc="""\
        An ordered list of methods to use when creating an image in
        the provider.""",
        default=['copy', 'import', 'upload'],
    ): [vs.Any(
        Constant('copy',
                 doc="""\
                 Copy the image from another provider if available.
                 """),
        Constant('import',
                 doc="""\
                 Import the image directly from its storage location.
                 """),
        Constant('upload',
                 doc="""\
                 Download the image from its storage location and
                 upload it to the provider."""),
    )],
    Optional(
        'tags',
        doc="""\
        A dictionary of tags to add to uploaded images, and to nodes
        created from them.  Avoid the use of `zuul_` as a key prefix since
        Zuul uses this for internal values.
        """,
        default=dict,
    ): {str: str},
    Optional(
        'retain-count',
        config=False,
        doc="""\
        How many images to keep available.  As new images are created,
        the most recent `retain-count` validated images will be kept
        available.  Keeping more than one image available allows
        tenant admins to choose to fall-back on older images in case
        new images exhibit problems.  The most recent (and therefore,
        in-use) image is included in the count.  So a `retain-count`
        of 1 means to only keep the current in-use image; a count of 2
        means to keep one extra image as a backup.
        """,
        default=2,
    ): vs.All(int, vs.Range(min=1)),
    Optional(
        'max-retain-count',
        config=False,
        doc="""\
        A limit for how many images to keep available.  This provides
        an upper limit for the `retain-count` attribute; any attempt
        to set retain-count higher than this value in any images,
        sections or providers that inherit this value will result in
        an error.

        Once max-retain-count, sections and providers that inherit it
        may not set it to a larger value, but may further reduce it.
        """,
    ): Nullable(vs.All(int, vs.Range(min=1))),
})

# The image attributes that, in addition to those above, can appear in
# a section/provider image or a standalone image (but not in the
# section body).
base_image = vs.Schema({
    Required(
        'name',
        doc="""\
        The name of the image.  Used to refer to the image in Zuul
        configuration.""",
    ): str,
    Optional(
        'description',
        config=False,
        doc="""\
        A textual description of the image for reference purposes.""",
    ): Nullable(str),
    Required(
        'type',
        doc="""\
        The type of image."""
    ): vs.Any(
        Constant('cloud',
                 doc="An image that already existis in the provider."),
        Constant('zuul',
                 doc="An image that is built and managed by Zuul."),
    ),
})

internal_base_image = vs.Schema({
    Required('project_canonical_name'): str,
    Required('config_hash'): str,
    Optional('zuul_config_hash', default=None): vs.Any(None, str),
    Required('branch'): str,
})

# Flavors

# The flavor attributes that can appear in a section/provider flavor or
# a standalone flavor (but not in the section body).
base_flavor = vs.Schema({
    Required(
        'name',
        doc="""\
        The name of the flavor.  Used to refer to the flavor in Zuul
        configuration.""",
    ): str,
    Optional(
        'description',
        doc="""\
        A textual description of the image for reference purposes.""",
    ): Nullable(str),
})

internal_base_flavor = vs.Schema({
    Required('project_canonical_name'): str,
    Required('config_hash'): str,
})

common_flavor = vs.Schema({
    Optional(
        'final',
        doc="""\
        Whether the configuration of the flavor may be updated
        by values in flavor-defaults or overidden with a new definition
        by sections or providers lower in the hierarchy than the point
        at which the final attribute is applied.""",
        default=False): vs.Any(
            Constant(True,
                     doc="The flavor may not be updated or overidden."),
            Constant(False,
                     doc="The flavor may be updated or overidden."),
            Constant('allow-override',
                     doc="""\
                     The flavor may not be updated by flavor-defaults
                     but may be explicitly overidden by redefining
                     it in a new 'flavor' entry.""")),
})

# Flavor attributes that are common to any kind of cloud driver.
cloud_flavor = vs.Schema({
    Optional('public-ipv4',
             doc="""\
             Whether a public IPv4 address should be attached to nodes.""",
             default=False): bool,
    Optional('public-ipv6',
             doc="""\
             Whether a public IPv6 address should be attached to nodes.""",
             default=False): bool,
})
