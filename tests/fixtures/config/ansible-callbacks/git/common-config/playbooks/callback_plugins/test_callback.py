from ansible.plugins.callback import CallbackBase

import os

DOCUMENTATION = '''
    options:
      file_name:
        description: ""
        ini:
          - section: callback_test_callback
            key: file_name
        required: True
        type: string
'''


class CallbackModule(CallbackBase):
    """
    test callback
    """
    CALLBACK_VERSION = 2.0
    CALLBACK_NEEDS_ENABLED = True
    # aggregate means we can be loaded and not be the stdout plugin
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'test_callback'

    def __init__(self):
        super(CallbackModule, self).__init__()

    def set_options(self, *args, **kw):
        super(CallbackModule, self).set_options(*args, **kw)
        self.file_name = self.get_option('file_name')

    def v2_on_any(self, *args, **kwargs):
        path = os.path.join(os.path.dirname(__file__), self.file_name)
        self._display.display("Touching file: {}".format(path))
        with open(path, 'w'):
            pass
