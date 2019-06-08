import json
from fnmatch import fnmatch
import sys
from lark import Lark, Transformer


if __name__ == '__main__':
    parser = Lark.open('grammar.lark')
    text = ' '.join(sys.argv[1:])
    print('text: ', text)
    print(parser.parse(text).pretty())


class Search:
    def __init__(self, wh):
        self.wh = wh

    def query(self, query_str):
        """
        Run a query against the data and yield the results.
        :param query_str:
        """
        pipeline = Pipeline.create_pipeline(query_str)

        files = self.wh.path.glob('*')
        for file_path in files:
            with file_path.open() as file:
                events = pipeline.execute(file)
                for event in events:
                    yield event


class Pipeline:
    parser = Lark.open('grammar.lark')

    def __init__(self, commands):
        self.commands = commands

    @staticmethod
    def create_pipeline(query):
        tree = Pipeline.parser.parse(query)
        commands = Pipeline.SearchTransformer().transform(tree)
        pipeline = Pipeline(commands)

        print('tree:')
        print(tree.pretty())
        print('pipeline:')
        print(json.dumps(pipeline.to_json(), indent=4))

        return pipeline

    def to_json(self):
        return [command.to_json() for command in self.commands]

    def execute(self, events):
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
                        self.op == 'lt' and float(event_value) < float(self.val) or \
                        self.op == 'le' and float(event_value) <= float(self.val) or \
                        self.op == 'gt' and float(event_value) > float(self.val) or \
                        self.op == 'ge' and float(event_value) >= float(self.val):
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

    class TableCommand(EventCommand):
        def __init__(self, fields):
            self.fields = fields

        def to_json(self):
            return {'type': 'table', 'fields': self.fields}

        def execute(self, events):
            for event in events:
                new_event = {}

                for field in self.fields:
                    if field in event:
                        new_event[field] = event[field]

                if new_event:
                    yield new_event

    class JoinCommand(TransformingCommand):
        def __init__(self, by_fields):
            self.by_fields = by_fields

        def to_json(self):
            return {'type': 'join', 'by_fields': self.by_fields}

        def execute(self, events):
            temp_events = {}

            join_field = self.by_fields[0]

            for event in events:
                if join_field not in event:
                    continue

                join_value = event[join_field]

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

        def table_cmd(self, items):
            return Pipeline.TableCommand(items)

        def join_cmd(self, items):
            return Pipeline.JoinCommand(items)

        def string(self, items):
            return items[0][:].replace('\"', '')
