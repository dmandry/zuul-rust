#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule


def main():
    module = AnsibleModule(
        argument_spec=dict(
            msg=dict(required=True, type='str'),
        )
    )
    module.exit_json(changed=False, msg=module.params['msg'])


if __name__ == '__main__':
    main()
