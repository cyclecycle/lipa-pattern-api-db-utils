import pickle
from spacy.tokens import Doc
from role_pattern_nlp.match import RolePatternMatch
from db import sql
import util


def load_sentence_doc(sentence_id, vocab=None, **kwargs):
    query = 'select spacy_doc from spacy_sentence_doc where sentence_id = ?'
    values = (sentence_id,)
    results = sql.db_query(query, values=values, fetch='one', **kwargs)
    spacy_doc_bytes = results[0]
    if not vocab:
        vocab = util.init_vocab()
    doc = Doc(vocab).from_bytes(spacy_doc_bytes)
    query = 'select * from tokens where sentence_id = ?'
    results = sql.db_query(query, values=values, **kwargs)
    tokens = sql.rows_to_dicts(results, 'tokens', **kwargs)
    tokens = [util.unpack_json_field(token, 'data') for token in tokens]
    for token in tokens:
        token_offset = token['token_offset']
        spacy_token = doc[token_offset]
        # Set custom extensions which are lost during seralisation
        custom_features = token['features'].get('_')
        util.set_token_extensions(spacy_token, custom_features)
    return doc


def load_role_pattern(pattern_id, **kwargs):
    row = sql.fetch_row('patterns', pattern_id, return_type='dict', **kwargs)
    if not row:
        raise Exception('No pattern found for id {}'.format(pattern_id))
    role_pattern_instance = row['role_pattern_instance']
    role_pattern = pickle.loads(role_pattern_instance)
    return role_pattern


def load_role_pattern_match(slots, match_tokens, sentence_id, **kwargs):
    match = spacify_match(slots, sentence_id, **kwargs)
    match_tokens = spacify_tokens(match_tokens, sentence_id, **kwargs)
    match = RolePatternMatch(match)
    match.match_tokens = match_tokens
    return match


def despacify_match(match, sentence_id, **kwargs):
    # Replace each token with its database representation
    slots = {}  # The labelled tokens
    for label, tokens in match.items():
        despacified_tokens = []
        for token in tokens:
            despacified_token = token_from_db(sentence_id, token.i, **kwargs)
            despacified_token = util.unpack_json_field(despacified_token, 'data')
            despacified_tokens.append(despacified_token)
        slots[label] = despacified_tokens
    match_tokens = match.match_tokens  # Includes the unlabelled tokens
    match_tokens = [token_from_db(sentence_id, token.i, **kwargs) for token in match_tokens]
    match_tokens = [util.unpack_json_field(token, 'data') for token in match_tokens]
    return slots, match_tokens


def spacify_match(match, sentence_id, **kwargs):
    for label, tokens in match.items():
        spacy_tokens = spacify_tokens(tokens, sentence_id, **kwargs)
        match[label] = spacy_tokens
    return match


def spacify_tokens(tokens, sentence_id, **kwargs):
    doc = load_sentence_doc(sentence_id, **kwargs)
    spacy_tokens = []
    for token in tokens:
        token_offset = token['token_offset']
        spacy_token = doc[token_offset]
        # Set custom extensions which are lost during seralisation
        custom_features = token['features'].get('_')
        util.set_token_extensions(spacy_token, custom_features)
        spacy_tokens.append(spacy_token)
    return spacy_tokens


def token_from_db(sentence_id, token_offset, **kwargs):
    query = 'select * from tokens where sentence_id = {0} and token_offset = {1}'.format(
        sentence_id, token_offset
    )
    token_row = sql.db_query(query, fetch='one', **kwargs)
    token_row = sql.row_to_dict(token_row, 'tokens', **kwargs)
    return token_row
