import json
import os
from fnmatch import fnmatch
from itertools import chain
from lark import Lark, Transformer
from collections.abc import Iterable


def query(query_str, wh, verbose=False):
    """
    Run a query against the data and yield the results.
    :param query_str:
    :param wh: the warehouse storing the data to query against
    :param verbose: print verbosely
    """
    if verbose:
        print(f'query: {query_str}')

    pipeline = Pipeline.create_pipeline(query_str, verbose)

    files = wh.path.glob('*')
    joined_files = [file_path.open() for file_path in files]
    data_in = chain(*joined_files)

    events = pipeline.execute(data_in)
    for event in events:
        yield event

    for file in joined_files:
        file.close()


class Pipeline:
    parser = Lark.open(os.path.join(os.path.dirname(__file__), 'grammar.lark'))

    def __init__(self, commands):
        self.commands = commands

    @staticmethod
    def create_pipeline(query_str, verbose=False):
        tree = Pipeline.parser.parse(query_str)
        commands = Pipeline.SearchTransformer().transform(tree)
        pipeline = Pipeline(commands)

        if verbose:
            print('tree:')
            print(tree.pretty())
            print('pipeline:')
            print(json.dumps(pipeline.to_json(), indent=4))

        return pipeline

    def to_json(self):
        return [command.to_json() for command in self.commands]

    def execute(self, events):
        if not isinstance(events, Iterable):
            raise Exception('events object is not iterable!')

        for command in self.commands:
            events = command.execute(events)

        return events

    class Command:
        def to_json(self):
            return ''

        def execute(self, events):
            return events

    class StreamingCommand(Command):
        pass

    class EventCommand(Command):
        pass

    class TransformingCommand(Command):
        pass

    class SearchCommand(StreamingCommand):
        def __init__(self, expressions):
            self.expressions = expressions

        def to_json(self):
            return {'type': 'search', 'expressions': [expression.to_json() for expression in self.expressions]}

        def execute(self, events):
            for event in events:
                if isinstance(event, str):
                    event = json.loads(event)

                for expression in self.expressions:
                    result = expression.evaluate(event)
                    if not result:
                        break
                else:
                    yield event

        class Expression:
            def to_json(self):
                return ''

            def evaluate(self, event):
                return True

        class ComparisonExpression(Expression):
            def __init__(self, field, val, op):
                self.field = field
                self.val = val
                self.op = op

            def to_json(self):
                return {'type': 'comparison', 'field': self.field, 'val': self.val, 'op': self.op}

            def evaluate(self, event):
                if self.field not in event:
                    return False

                event_value = str(event[self.field])

                if self.op == 'eq' and not fnmatch(event_value, self.val) or \
                        self.op == 'ne' and fnmatch(event_value, self.val) or \
                        self.op == 'lt' and float(event_value) >= float(self.val) or \
                        self.op == 'le' and float(event_value) > float(self.val) or \
                        self.op == 'gt' and float(event_value) <= float(self.val) or \
                        self.op == 'ge' and float(event_value) < float(self.val):
                    return False

                return True

        class DisjunctionExpression(Expression):
            def __init__(self, parts):
                self.parts = parts

            def to_json(self):
                return {'type': 'disjunction', 'parts': [part.to_json() for part in self.parts]}

            def evaluate(self, event):
                for part in self.parts:
                    if part.evaluate(event):
                        return True

                return False

        class NotExpression(Expression):
            def __init__(self, item):
                self.item = item

            def to_json(self):
                return {'type': 'not', 'item': self.item.to_json()}

            def evaluate(self, event):
                if self.item.evaluate(event):
                    return False

                return True

    class FieldsCommand(EventCommand):
        def __init__(self, fields):
            self.fields = fields

        def to_json(self):
            return {'type': 'fields', 'fields': self.fields}

        def execute(self, events):
            for event in events:
                new_event = {}

                for field in self.fields:
                    if field in event:
                        new_event[field] = event[field]

                if new_event:
                    yield new_event

    class JoinCommand(TransformingCommand):
        def __init__(self, by_field):
            self.by_field = by_field

        def to_json(self):
            return {'type': 'join', 'by_field': self.by_field}

        def execute(self, events):
            temp_events = {}

            for event in events:
                if self.by_field not in event:
                    continue

                join_value = event[self.by_field]

                if join_value not in temp_events:
                    temp_events[join_value] = event
                else:
                    existing_event = temp_events[join_value]
                    for field, value in event.items():
                        if field not in existing_event:
                            existing_event[field] = value
                        else:
                            existing_value = existing_event[field]
                            if not isinstance(existing_value, list) and existing_value != value:
                                existing_event[field] = [existing_value, value]
                            elif value not in existing_value:
                                existing_event[field].append(value)

            for event in temp_events.values():
                yield event

    class PrettyprintCommand(TransformingCommand):
        def __init__(self, format_type):
            self.format_type = format_type

        def to_json(self):
            return {'type': 'prettyprint', 'format': self.format_type}

        def execute(self, events):
            if self.format_type == 'json':
                for event in events:
                    yield json.dumps(event, indent=4)
            elif self.format_type == 'table':
                for event in self.pretty_print_table(events):
                    yield event

        def pretty_print_table(self, events):
            fields_dict = {}

            events_list = []

            for event in events:
                for field, value in event.items():
                    value = str(value)
                    value_len = len(value)

                    if field not in fields_dict:
                        fields_dict[field] = {'position': len(fields_dict) + 1, 'len': value_len}
                    else:
                        if value_len > fields_dict[field]['len']:
                            fields_dict[field]['len'] = value_len

                events_list.append(event)

            fields = [{'name': d[0], **d[1]} for d in sorted(fields_dict.items(), key=lambda x: x[1]['position'])]

            header = ''

            for field in fields:
                field_len = len(field['name'])
                if field_len > field['len']:
                    field['len'] = field_len

                header += field['name'].ljust(field['len'] + 2)

            yield header

            for event in events_list:
                line = ''

                for field in fields:
                    field_name = field['name']
                    value = ''
                    if field_name in event:
                        value = str(event[field_name])

                    line += value.ljust(field['len'] + 2)

                yield line

    class SearchTransformer(Transformer):
        start = list

        def search_cmd(self, items):
            return Pipeline.SearchCommand(items)

        def comparison_expr(self, items):
            return Pipeline.SearchCommand.ComparisonExpression(items[0], items[2], items[1])

        def eq(self, items):
            return 'eq'

        def ne(self, items):
            return 'ne'

        def lt(self, items):
            return 'lt'

        def le(self, items):
            return 'le'

        def gt(self, items):
            return 'gt'

        def ge(self, items):
            return 'ge'

        def disjunction_expr(self, items):
            return Pipeline.SearchCommand.DisjunctionExpression(items)

        def not_expr(self, items):
            return Pipeline.SearchCommand.NotExpression(items[0])

        def fields_cmd(self, items):
            return Pipeline.FieldsCommand(items)

        def join_cmd(self, items):
            return Pipeline.JoinCommand(items[0])

        def pp_cmd(self, items):
            return Pipeline.PrettyprintCommand(items[0])

        def json(self, items):
            return 'json'

        def table(self, items):
            return 'table'

        def string(self, items):
            return items[0][:].replace('\"', '')
