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
        self.parser = Lark.open('grammar.lark')
        self.transformer = SearchTransformer()

    def query(self, query_str):
        """
        Run a query against the data and yield the results.
        :param query_str:
        """
        tree = self.parser.parse(query_str)
        pipeline = self.transformer.transform(tree)

        print('tree:')
        print(tree.pretty())
        print('pipeline:')
        print(json.dumps(pipeline, indent=4))

        first_cmd = pipeline[0]

        if first_cmd['type'] != 'search':
            raise Exception('search command must come first!')

        files = self.wh.path.glob('*')
        for file_path in files:
            with file_path.open() as file:
                for line in file:
                    event = json.loads(line)
                    if self._filter_event(event, first_cmd['expressions']):
                        yield event

    def _filter_event(self, event, expressions):
        """
        Filter an event.
        :param event:
        :param raw_event: the event in string format
        :param field_filters: filters involving a key, value pair
        :param text_filters: filters involving plain text
        :return: True if the event matches the filters; False if not
        """
        for expression in expressions:
            result = self._eval_expression(event, expression)
            if not result:
                return False

        return True

    def _eval_expression(self, event, expression):
        if expression['type'] == 'comparison':
            field = expression['key']

            if field not in event:
                return False

            value = expression['val']
            op = expression['op']

            event_value = str(event[field])

            if op == 'eq' and not fnmatch(event_value, value) or \
                    op == 'ne' and fnmatch(event_value, value) or \
                    op == 'lt' and float(event_value) < float(value) or \
                    op == 'le' and float(event_value) <= float(value) or \
                    op == 'gt' and float(event_value) > float(value) or \
                    op == 'ge' and float(event_value) >= float(value):
                return False
        elif expression['type'] == 'disjunction':
            for part in expression['parts']:
                if self._eval_expression(event, part):
                    break
            else:
                return False
        elif expression['type'] == 'not':
            if self._eval_expression(event, expression['item']):
                return False

        return True


class SearchTransformer(Transformer):
    start = list

    def search_cmd(self, items):
        return {'type': 'search', 'expressions': items}

    def comparison_expr(self, items):
        return {'type': 'comparison', 'key': items[0], 'val': items[2], 'op': items[1]}

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
        return {'type': 'disjunction', 'parts': items}

    def not_expr(self, items):
        return {'type': 'not', 'item': items[0]}

    def table_cmd(self, items):
        return {'type': 'table', 'fields': items}

    def join_cmd(self, items):
        return {'type': 'join', 'by_fields': items}

    def string(self, items):
        return items[0][:]
