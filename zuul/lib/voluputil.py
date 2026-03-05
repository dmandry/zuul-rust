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

# This adds some helpers that are useful for mutating hyphenated YAML
# structures into underscored python dicts.

import voluptuous as vs


UNDEFINED = object()


def assemble(*schemas, doc=None):
    """Merge any number of voluptuous schemas into a single schema.  The
    input schemas must all be dictionary-based.

    """
    ret = vs.Schema({})
    old_doc = None
    for x in schemas:
        ret = ret.extend(x.schema)
        if schema_doc := getattr(x, 'doc', None):
            old_doc = schema_doc
    doc = doc or old_doc
    if doc:
        ret.doc = doc
    return ret


def discriminate(comparator):
    """Chose among multiple schemas in a Union using a supplied comparator
    function.  This does some extra work to find the interesting part
    of nested schemas.  It follows nested schemas down and picks the
    first one of each.  This makes it suitable for use with All
    combined with RequiredExclusive.  This is intended to be passed to
    the discriminant argument of Union.

    The comparator is called with the following arguments:

    :param object val: The input data to be validated.
    :param Schema s: The first schema with values likely to be comparable.

    """
    def disc(val, alt):
        ret = []
        for a in alt:
            s = a
            while hasattr(s, 'validators'):
                s = s.validators[0]
            while hasattr(s, 'schema'):
                s = s.schema
            if comparator(val, s):
                ret.append(a)
        return ret
    return disc


class RequiredExclusive:
    """A validator to require that at least one of a set of alternates is
    present.  Use this in combination with Exclusive and All (since
    Exclusive doesn't imply Required).  Example:

    All(
        {Exclusive(...), Exclusive(...)},
        RequiredExclusive(...)
    )
    """
    def __init__(self, *attributes, msg=None, doc=None):
        self.schema = vs.Schema({
            # Use the mutated names for the requirements
            vs.Required(
                vs.Any(*attributes),
                msg=msg,
            ): object,
            object: object,
        })
        self.doc = doc

    def __call__(self, v):
        return self.schema(v)


class Required(vs.Required):
    """Require an attribute and mutate its name

    This mutates the name of the attribute to lowercase it and replace
    hyphens with underscares.  This enables the output of the
    validator to be used directly in setting python object attributes.

    :param str output: Override the output name of the attribute
    :param str doc: A docstring for the attribute
    :param bool config: Whether the attribute represents a
      configuration attribute inherent to the object's identity (used
      for image config hashes)

    """
    def __init__(self, schema, default=UNDEFINED, output=None, doc=None,
                 config=True):
        if not isinstance(schema, str):
            raise Exception("Only strings are supported")
        super().__init__(schema, default=default)
        if output is None:
            output = str(schema).replace('-', '_').lower()
        self.output = output
        self.doc = doc
        self.config = config

    def __call__(self, data):
        # Superclass ensures that data==schema
        super().__call__(data)
        # Return our mutated form
        return self.output


class Optional(vs.Optional):
    """Mark an attribute optional and mutate its name

    This mutates the name of the attribute to lowercase it and replace
    hyphens with underscares.  This enables the output of the
    validator to be used directly in setting python object attributes.

    This works in conjuction with Nullable.

    :param str output: Override the output name of the attribute
    :param str doc: A docstring for the attribute
    :param bool config: Whether the attribute represents a
      configuration attribute inherent to the object's identity (used
      for image config hashes)

    """
    def __init__(self, schema, default=UNDEFINED, output=None, doc=None,
                 config=True):
        if not isinstance(schema, str):
            raise Exception("Only strings are supported")
        super().__init__(schema, default=default)
        if output is None:
            output = str(schema).replace('-', '_').lower()
        self.output = output
        self.doc = doc
        self.config = config

    def __call__(self, data):
        # Superclass ensures that data==schema
        super().__call__(data)
        # Return our mutated form
        return self.output


class Nullable:
    """Set the output value to None when no input is supplied.

    When used with Optional, if no input value is supplied, this will
    set the output to None without also accepting None as an input
    value.

    """
    def __init__(self, schema):
        self.schema = vs.Schema(schema)

    def __call__(self, v):
        if v is UNDEFINED:
            return None
        return self.schema(v)


class AsList:
    """Accept either the specified value or a list of it.

    Always return a list.
    """
    def __init__(self, schema):
        self.schema = vs.Any([schema], schema)

    def __call__(self, v):
        val = self.schema(v)
        if not val:
            return []
        if isinstance(val, list):
            return val
        return [val]


class Constant:
    """A string constant with a documentation attribute

    Use this instead of a bare string when you want to supply
    documentation.

    """
    def __init__(self, schema, doc=None):
        self.schema = vs.Schema(schema)
        self.schema.doc = doc

    def __call__(self, v):
        return self.schema(v)

    def __eq__(self, other):
        return str(self.schema) == other


class Exclusive(vs.Exclusive):
    """Subclass of Exclusive that supports documentation
    """
    def __init__(self, schema, group_of_exclusion, msg=None,
                 description=None, doc=None):
        super().__init__(schema, group_of_exclusion, msg, description)
        self.doc = schema.doc
