#! /usr/bin/python3
# vim: set fileencoding=<utf-8> :
"""
...things get weird when you write code at odd hours...

convert_sql.py  --  A tool that converts a raw sql file into sql queries for application use
note the source file MUST abide by the following rules:

1. At the beginning of each file there may be a set of sql line comments (lines starting with "--")
    that defines various required and optional configuration options  as a key:value separated pair as described below:
      -  "DIALECT" (required)  -- specifies the sql dialect used. accepted values include: "postgres", "base_sql"
         eg: -- DIALECT : postgres
      - "ENDHEAD ::" (required) -- signifies end of header segment
      -  "VERSION" (optional) -- adds a version tag information to the output file
         eg: -- VERSION : 0.0.1
      -  "OUTPUT" (optional) -- the base path to deposit output file. accepts relative or absolute paths
         eg: -- OUTPUT : path/to/out/dir/

2. preceding any desired sql statement(s), there is an optional block comment "/* ... */ " that can contain general information and other supported
    special characters/sequences that provide any meta data/documentation for the sql statement
    supported sequences include:
        - "EXCLUDE" -- marks the associated sql statement to NOT be converted to the output format. used for hiding statements from begin output
        - "MULTILINE" -- keeps the sql query as a multiline statement instead of compressing to single line form.
        - "OUTPUT" -- the output file name to be grouped into -- added to header defined OUTPUT

3. statements can be grouped using isolated block comments (block comments with no sql on the next line) that employ the STARTGROUP and ENDGROUP markers.
    group tags consist of up to three parts:
        1. the STARTGROUP or ENDGROUP marker
        2. the group name in either
            - snake_case
            - CamelCase
        3. an optional third tag OUTPUT that describes the output file name (excluding extension) -- if included, only include in STARTGROUP
    eg: /* STARTGROUP : group_name_here : OUTPUT : /appended/output/path/output_file_name */
            ...
          /* ENDGROUP : group_name_here */
"""
import re
import os
import sys
import json
from copy import deepcopy  # so you know its gonna some fun memory usage ahead
from os import path as osp  # why some ppl dont alias this ill never get...
from inspect import cleandoc
from enum import Enum, auto
from collections import deque, namedtuple

# meta info constants
LINE_COMMENT_START = '--'
BLOCK_COMMENT_START = '/*'
BLOCK_COMMENT_END = '*/'
SQL_END = ';'
END_HEADER_MARKER = 'endhead'
REQUIRED_HEADER_FIELDS = {'dialect'}
HEADER_FIELDS =  {'version', 'output'} | REQUIRED_HEADER_FIELDS | {END_HEADER_MARKER}
HEADER_REGEX = re.compile(r'\s*--\s*(?P<tag>{})\s*:\s*(?P<value>.*)'.format('|'.join(HEADER_FIELDS)),re.I)

STARTGROUP_TAG = 'startgroup'
ENDGROUP_TAG = 'endgroup'
GROUP_REGEX = re.compile(r'\s*/\*\s*(?P<tag>{})\s*:\s*(?P<name>.*?)'
                                             r'\s*(?::\s*OUTPUT\s*:\s*(?P<output>.*?))?\s*\*/\s*$'.format('|'.join([STARTGROUP_TAG,
                                                                                                                                                      ENDGROUP_TAG])),
                                                                                                                             re.I)

META_FIELDS = {'EXCLUDE', 'MULTILINE', 'OUTPUT'}
META_COMMENT_REGEX = re.compile(r'/*.*?(EXCLUDE|MULTILINE)|(OUTPUT\s*:\s*(?:.*)).*\*/',re.I|re.M|re.S)
RAW_META_COMMENT_REGEX = re.compile(r'.*?(EXCLUDE|MULTILINE)|(OUTPUT\s*:\s*(?:.*)).*',re.I|re.M|re.S)
META_OUTPUT_REGEX = re.compile(r'\s*(?P<tag>OUTPUT)\s*:\s*(?P<value>.*)',re.I)

# file info constants
LINE_COMMENT_REGEX = re.compile(r'\s*--\s+(?P<contents>.*)$', re.I)
BLOCK_COMMENT_REGEX =  re.compile(r'\s*/*\*\s*(?P<contents>.*?)\s*\*/\s*', re.I|re.S)
SQL_STARTERS = {'SELECT','PREPARE','CREATE','DROP','WITH', 'EXECUTE', 'RAISE'}

# PREPARE statement parsing for parametrized queries
PREPARE_REGEX = re.compile(r'PREPARE\s+(\w+)(?:\s*\(.+\))?\s+AS\s+(.*);',re.I|re.S)


class TokenType(Enum):
    SQL = auto()
    LINE_COMMENT = auto()
    BLOCK_COMMENT = auto()
    HEADER = auto()
    GROUP_TAG = auto()
    META_COMMENT = auto()

COMMENT_TOKENS = {TokenType.META_COMMENT, TokenType.LINE_COMMENT, TokenType.BLOCK_COMMENT}
Token = namedtuple('Token', 'type contents' )

def flatten(big_list):
    return [item for sublist in big_list for item in sublist]

def index_containing_substring(the_list, substring):
    for i, s in enumerate(the_list):
        if substring in s.lower():
              return i
    return -1

def tokenizer(filename):
    """
    generator that reads tokens from input file
    parses file into:
      - single line comments
      - header directives
      - block comments
      - meta comments
      - sql queries

    tosses blank lines -- modify behaviour  where # yield 'EMPTY' is 
    """
    with open(filename, 'rt') as file:
        block = [next(file)]  # read what should be the first number
        while True:
            if not block[-1].strip():                       # blank lines
                # yield 'EMPTY'
                pass
            elif block[-1].lstrip().startswith(LINE_COMMENT_START):  # line comment
                header = HEADER_REGEX.match(block[-1])
                if header:
                    yield Token(TokenType.HEADER, header.groupdict())
                else:
                    yield Token(TokenType.LINE_COMMENT, LINE_COMMENT_REGEX.match(block[-1])['contents'])
            elif block[-1].lstrip().startswith(BLOCK_COMMENT_START): # block comment
                if block[-1].rstrip().endswith(BLOCK_COMMENT_END): # single line block comment
                    group = GROUP_REGEX.match(block[-1])
                    if group:
                        yield Token(TokenType.GROUP_TAG, group.groupdict())
                    else:
                        yield Token(TokenType.BLOCK_COMMENT, BLOCK_COMMENT_REGEX.match(block[-1])['contents'])
                else:                                              # multi line block comment
                    block.append(next(file))
                    while not block[-1].strip().endswith(BLOCK_COMMENT_END):
                        block.append(next(file))
                    join_block = ''.join(block)
                    meta_list =  META_COMMENT_REGEX.search(join_block)
                    if meta_list:
                        yield Token(TokenType.META_COMMENT, cleandoc(BLOCK_COMMENT_REGEX.match(join_block)['contents']))
                    else:
                        yield Token(TokenType.BLOCK_COMMENT, cleandoc(BLOCK_COMMENT_REGEX.match(join_block)['contents']))
            elif block[-1].lstrip().split(' ',1)[0] in SQL_STARTERS:  # sql query
                if block[-1].rstrip().endswith(SQL_END):   # single line sql query
                    yield Token(TokenType.SQL, block[-1].strip())
                else:                                                          # multiline sql query
                    block.append(next(file))
                    while not block[-1].rstrip().endswith(SQL_END):
                        block.append(next(file))
                    yield Token(TokenType.SQL, cleandoc(' '.join(block)))
            block = [next(file)]

class BaseConverter(object):
    """
    converts the contents of a *formatted* sql file into raw string
    to be mapped into whatever output implemented by the appropriate subclass
    """
    def __init__(self, source_file, dialect=None):
        self.source = source_file
        self.sql_keywords = SQL_STARTERS
        self.config = {'outfile_type': '.txt', 'allow_comments': True}
        self.outfiles = {}
        self.token_source = tokenizer(self.source)
        self.tokens = None

    def process_header(self):
        """
        note if no header provided, this WILL toss all lines...which is not good
        """
        done = False
        while not done:
            token = next(self.token_source)
            if token.type is not TokenType.HEADER: # skip comments inside header block
                pass
            elif token.contents['tag'].lower() != END_HEADER_MARKER:
                self.config[token.contents['tag'].lower()] = token.contents['value']
            else:
                done = True
        if 'output' in self.config:
            self.config['output'] = osp.normcase(self.config['output'])
        else:
            self.config['output'] = os.getcwd()
        # check of header required fields are populated
        for header in REQUIRED_HEADER_FIELDS:
            if header not in  self.config:
                raise LookupError('Missing a required field: {!r}'.format(header))

    def process_token(self,token, existing_params={}):
        """
        return dict of the processed token, type, and extra dict of pre-existing params dict
        """
        # print(token)
        processed_token = {}
        if token.type in {TokenType.LINE_COMMENT, TokenType.BLOCK_COMMENT}:  # is normal comment
            if  existing_params['allow_comments']:
                processed_token = {'contents' : token.contents , 'type' : token.type, 'existing_params' : deepcopy(existing_params)}
            else:
                pass
        elif token.type is TokenType.GROUP_TAG:  # grouping tokens
            if token.contents['tag'].lower() == STARTGROUP_TAG:
                if  token.contents['output']:
                    existing_params['output'] = osp.normcase(osp.join(osp.dirname(existing_params['output']), token.contents['output']+self.config['outfile_type']))
                elif  not osp.splitext(existing_params['output'])[1]:
                        existing_params['output'] = osp.join(existing_params['output'],'default'+self.config['outfile_type'])
                    
                existing_params['group'].append(token.contents['name'])
            else:  # end tag -> detract path
                existing_params['group'].pop()
                if not existing_params['group']: # if empty group name -> default
                    token.contents['output'] = existing_params['output']
                    existing_params['output'] = osp.join(osp.dirname(existing_params['output']),'default'+self.config['outfile_type'])
                else:
                    existing_params['output'] = osp.dirname(existing_params['output'])+self.config['outfile_type']
            processed_token = {'contents':token.contents, 'type': token.type, 'existing_params':deepcopy(existing_params)}
        elif token.type is TokenType.META_COMMENT:  # is a meta comment
            raw_meta = flatten(RAW_META_COMMENT_REGEX.findall(token.contents))
            output_provided = index_containing_substring(raw_meta, 'output')
            if  output_provided != -1:
                new_output = META_OUTPUT_REGEX.match(raw_meta.pop(output_provided))['value']
                base, ext = osp.splitext(existing_params['output'])
                existing_params['output'] = osp.join(base, new_output + (ext if ext else self.config['outfile_type']))
                raw_meta.append('clear_out')
            meta_cmds = map(str.lower, filter(None,raw_meta))
            existing_params['meta'] = meta_cmds
            sql_token = next(self.token_source)
            if sql_token.type is not TokenType.SQL:
                raise ValueError('Meta comment not preceeding sql token. meta token followed by {!s} token.'.format(sql_token.type.name))
            else:
                processed_token, existing_params = self.process_token(sql_token, deepcopy(existing_params))  # recursion IRL?
        elif token.type is TokenType.SQL:  # is SQL
            meta= existing_params.pop('meta',[])
            formatted_sql = re.sub(r'(\s+)|(\s*--\s*.*\s+)', ' ', token.contents).strip()
            if 'exclude' in meta:
                processed_token = {}
            elif 'multiline' in meta:
                formatted_sql = token.contents.strip()
            if 'clear_out' in meta:
                # existing_params['output'] = osp.
                pass
            processed_token = {'contents': formatted_sql, 'type':token.type, 'existing_params': deepcopy(existing_params)}
        else:  # something went wrong...
            raise ValueError('Unknown Token: {!r}'.format(token))
        return processed_token, existing_params

    def process_tokens(self):  # generator for yeilding processed tokens
        self.process_header()
        try:
            pre_existing_params = {'output': self.config['output'],   # will fail if process_header has not been called
                                                    'group': [], 
                                                    'allow_comments':self.config.get('allow_comments', True)} 
        except KeyError:
            raise ValueError('could not find output configuration -- did you call process_header first?')
        for token in self.token_source:
            processed_token, pre_existing_params = self.process_token(token, existing_params=pre_existing_params)
            if processed_token:
                # print(processed_token)
                yield processed_token
            else:  # skip empty tokens
                continue

    def get_outfile(self, outfile_path, clear=True):
        """
        gets outfile pointer. 
        if not already opened creates one
        """
        outfile_path = osp.normcase(outfile_path) 
        if not outfile_path in self.outfiles: 
            os.makedirs(osp.dirname(outfile_path), exist_ok=True)
            self.outfiles[outfile_path] = open(outfile_path, 'w+')
            if clear: self.outfiles[outfile_path].truncate()
        return self.outfiles[outfile_path]
    
    def output_token(self, token, outfile):
        raise NotImplementedError('Should Be implemented via a subclass')
    
    def output_tokens(self):
        for token in self.process_tokens():
            if token['type'] is not TokenType.GROUP_TAG or token['contents']['tag'].lower() == STARTGROUP_TAG:  # leverage short ciruit behaviour of or operator
                outfile = self.get_outfile(token['existing_params']['output'])
            elif token['contents']['tag'].lower() == ENDGROUP_TAG:
                outfile = self.get_outfile(token['contents']['output'] or token['existing_params']['output'])
            self.output_token(token, outfile)
    
    def __del__(self):
        for outfile in self.outfiles:
            try:
                outfile.close()
            except:
                pass
        
class  JSConverter(BaseConverter):
    """
    keeps comments
    uses group tags to space out lines
    """
    def __init__(self, *args,**kwargs):
        super().__init__(*args, **kwargs)
        self.config['outfile_type'] = '.js'

class JSONConverter(BaseConverter):
    """
    tosses all comments
    uses groups to make nested dicts
    only converts PREPARE sql statements:
        use name inside the PREPARE portion as key name
        the parametrized query ready for the pg api
    """
    def __init__(self, *args, json_spaces=4, **kwargs):
        super().__init__(*args, **kwargs)
        self.config['outfile_type'] = '.json'
        self.config['allow_comments'] = False
        self.config['json_space'] = ' '*json_spaces 
        self.last_indent = None
        self.last_line = ''
         
    def get_outfile(self,outfile):
        outfile = super().get_outfile(outfile)
        if not outfile.tell():
            outfile.write('{\n')
            outfile.write('{}"version": {},\n'.format(self.config['json_space'], json.dumps(self.config['version'])))
            self.last_indent = self.config['json_space']
        return outfile

    def comma_check(self, outfile):
        prev_line = self.last_line.rstrip()
        if prev_line.endswith(','): 
            seek_dist = len(self.last_indent) - 1 
            outfile.seek(outfile.tell()-seek_dist)
            outfile.write('\n')

    def output_token(self, token, outfile):
        # print(token)
        if token['type'] is TokenType.SQL:
            if not token['contents'].lower().startswith('prepare'):
                pass
            else:
                key,query = PREPARE_REGEX.match(token['contents']).groups()
                query = query.replace('\n', ' ')  # flatten any multiline SQL 
                line = '{}"{}": {},\n'.format(self.last_indent, key, json.dumps(query))
                outfile.write(line)
                self.last_line = line
        elif token['type'] is TokenType.GROUP_TAG:
            if token['contents']['tag'].lower() == STARTGROUP_TAG:  # create new nested dict with key as group name
                line = '{0}"{1}": {{\n'.format(self.last_indent, token['contents']['name'])
                outfile.write(line)
                self.last_line = line
                self.last_indent += self.config['json_space']
            else:  # endgroup token
                self.last_indent = self.last_indent[:len(self.config['json_space'])]
                self.comma_check(outfile)
                line = '{}}},\n'.format(self.last_indent)
                outfile.write(line)
                self.last_line = line
    
    
    def output_tokens(self):
        super().output_tokens()
        for outfile in self.outfiles.values():
            self.comma_check(outfile)
            outfile.write('}')


if __name__ == '__main__':
    from pprint import pprint
    try: 
        infile = sys.argv[1]
    except IndexError:  
        infile = 'test.txt'
    conv = JSONConverter(infile)
    # conv.process_header()
    # for token in conv.process_tokens():
        # pprint(token)
    conv.output_tokens()
    