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

# Print out a representation of a provider configuration schema.

import argparse
import textwrap

from zuul.driver.openstack import openstackprovider
from zuul.driver.aws import awsprovider
from zuul.driver.azure import azureprovider
from zuul.driver.static import staticprovider
from zuul.driver.kubernetes import kubernetesprovider
from zuul.lib.voluputil import AsList, Nullable, Constant, UNDEFINED

import voluptuous as vs
import yaml


class IndentedListDumper(yaml.Dumper):
    def increase_indent(self, flow=False, *args, **kwargs):
        return super().increase_indent(flow=flow, indentless=False)


class SchemaWalker:
    def __init__(self, schema, driver, doc=None):
        self.schema = schema
        self.driver = driver
        self.doc = doc or getattr(schema, 'doc', None)

    def toDict(self):
        if isinstance(self.schema, str):
            return dict(type="const", value=self.schema, doc=self.doc)
        if self.schema is True:
            return dict(type="const", value=self.schema, doc=self.doc)
        if self.schema is False:
            return dict(type="const", value=self.schema, doc=self.doc)
        if self.schema == str:  # noqa
            return dict(type="str", doc=self.doc)
        if self.schema == int:  # noqa
            return dict(type="int", doc=self.doc)
        if self.schema == bool:  # noqa
            return dict(type="bool", doc=self.doc)
        if self.schema == float:  # noqa
            return dict(type="float", doc=self.doc)
        if self.schema == dict:  # noqa
            return dict(type="dict", doc=self.doc)
        if self.schema == None:  # noqa
            return dict(type="const", value='null', doc=self.doc)
        if isinstance(self.schema, Constant):
            w = SchemaWalker(self.schema.schema, self.driver)
            return w.toDict()
        if isinstance(self.schema, Nullable):
            w = SchemaWalker(self.schema.schema, self.driver, self.doc)
            return w.toDict()
        if isinstance(self.schema, vs.Schema):
            w = SchemaWalker(self.schema.schema, self.driver, self.doc)
            return w.toDict()
        if isinstance(self.schema, AsList):
            # Use the first alt (the list); ignore the second.
            w = SchemaWalker(self.schema.schema.validators[0],
                             self.driver, self.doc)
            return w.toDict()
        if isinstance(self.schema, vs.Any):
            ret = []
            for v in self.schema.validators:
                w = SchemaWalker(v, self.driver)
                v = w.toDict()
                ret.append(v)
            islist = False
            if len(ret) == 1 and isinstance(ret[0], list):
                ret = ret[0]
                islist = True
            return dict(alts=ret, doc=self.doc, list=islist)
        if isinstance(self.schema, vs.All):
            schemas = []
            for v in self.schema.validators:
                w = SchemaWalker(v, self.driver)
                v = w.toDict()
                schemas.append(v)
            ret = {}
            for x in schemas:
                if isinstance(x, dict):
                    ret.update(x)
            if not ret.get('doc'):
                ret['doc'] = self.doc
            return ret
        if isinstance(self.schema, vs.Union):
            ret = []
            for v in self.schema.validators:
                w = SchemaWalker(v, self.driver)
                v = w.toDict()
                ret.append(v)
            return dict(alts=ret, doc=self.doc)
        if isinstance(self.schema, dict):
            ret = {}
            for k, v in self.schema.items():
                name = str(k)
                if '_' in name:
                    continue
                w = SchemaWalker(v, self.driver, getattr(k, 'doc', None))
                v = w.toDict()
                if (hasattr(k, 'default') and
                    k.default not in [UNDEFINED, vs.UNDEFINED]):
                    d = k.default()
                    if (d not in [UNDEFINED, vs.UNDEFINED] and
                        d != dict() and
                        d != list()):
                        v['default'] = d
                ret[name] = v
            return dict(type='dict', doc=self.doc, value=ret)
        if isinstance(self.schema, list):
            elements = []
            for v in self.schema:
                w = SchemaWalker(v, self.driver)
                v = w.toDict()
                if isinstance(v, list):
                    elements.extend(v)
                else:
                    elements.append(v)
            if len(elements) == 1 and not elements[0].get('doc'):
                ret = elements[0].copy()
                ret['doc'] = self.doc
                ret['list'] = True
                return ret
            return elements

    def _attr(self, indent, name, data):
        indent1 = indent * '   '
        indent2 = (indent + 1) * '   '
        kind = data.get('type')
        if not kind and data.get('list'):
            kind = 'list'
        out = f"{indent1}.. attr:: {name}\n"
        if kind and kind not in ['const']:
            out += f"{indent2}:type: {kind}\n"
        if 'default' in data:
            out += f"{indent2}:default: {data['default']}\n"
        if data.get('required'):
            out += f"{indent2}:required:\n"
        out += '\n'
        if data.get('doc'):
            doc = textwrap.dedent(data['doc'])
            doc = textwrap.indent(doc, indent2)
            doc = doc.replace("$driver", self.driver)
            out += doc
            while not out.endswith('\n\n'):
                out += '\n'
        else:
            out += "TODO: document\n\n"
        if data.get('value'):
            out += self._toDoc(indent + 1, data)
        if data.get('alts'):
            for alt in data['alts']:
                # This is not generic, it only works for images
                if name == 'images':
                    disc = alt['value']['type']['alts'][0]['value']
                    altname = f"{name}[{disc}]"
                    out += self._attr(indent, altname, alt)
                else:
                    out += self._toDoc(indent + 1, alt)
        return out

    def _value(self, indent, data):
        indent1 = indent * '   '
        indent2 = (indent + 1) * '   '
        value = data['value']
        out = f"{indent1}.. value:: {value}\n"
        out += '\n'
        if data.get('doc'):
            doc = textwrap.dedent(data['doc'])
            doc = textwrap.indent(doc, indent2)
            doc = doc.replace("$driver", self.driver)
            out += doc
            while not out.endswith('\n\n'):
                out += '\n'
        else:
            out += "TODO: document\n\n"
        return out

    def _toDoc(self, indent, data):
        out = ''
        if data.get('type') == 'dict':
            value = data['value']
            for k in sorted(value.keys()):
                v = value[k]
                # Free-form dicts like "tags" end up with this
                if k == "<class 'str'>":
                    continue
                # Special case for AWS quota resources
                if k.startswith("L-"):
                    continue
                if v.get('doc') == "UNDOCUMENTED":
                    continue
                out += self._attr(indent, k, v)
        elif data.get('type') == 'const':
            out += self._value(indent, data)
        return out

    def toDoc(self):
        data = self.toDict()
        out = ''

        provider = {
            'value': {
                f'provider[{self.driver}]': data,
            },
            'type': 'dict',
        }
        out += self._toDoc(0, provider)
        return out


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Print provider configuration schema")
    parser.add_argument('--yaml', dest='yaml', action='store_true',
                        help='Output YAML')
    parser.add_argument(dest='driver',
                        help='The driver to inspect')
    args = parser.parse_args()
    if args.driver == 'aws':
        ps = awsprovider.AwsProviderSchema()
    elif args.driver == 'openstack':
        ps = openstackprovider.OpenstackProviderSchema()
    elif args.driver == 'static':
        ps = staticprovider.StaticProviderSchema()
    elif args.driver == 'azure':
        ps = azureprovider.AzureProviderSchema()
    elif args.driver == 'kubernetes':
        ps = kubernetesprovider.KubernetesProviderSchema()
    else:
        raise Exception("Unknown driver")
    s = ps.getProviderSchema()
    w = SchemaWalker(s, args.driver)
    if args.yaml:
        out = w.toDict()
        print(yaml.dump(out, Dumper=IndentedListDumper,
                        default_flow_style=False))
    else:
        out = w.toDoc()
        print(out)
