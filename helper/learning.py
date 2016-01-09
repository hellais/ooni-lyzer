from pprint import pprint
import constants
import collections
import jellyfish
import unittest
import logging

Suggestion = collections.namedtuple('Suggestion', ['a', 'b', 'edit_distance'])


def remove_duplicates(dictionary, relocate_to):
    duplicates = set(dictionary.keys()).intersection(set(dictionary[relocate_to].keys()))
    if not duplicates:
        return dictionary
    else:
        for k in duplicates:
            if dictionary[k] == dictionary[relocate_to][k]:
                logging.warning("Removing duplicate (k, v) pair with key: %s" % k)
                del dictionary[relocate_to][k]
        else:
            return dictionary


def autocorrect(dictionary, required, relocate_to):
    """
    This performs report normalisation without relying on hard-coded values - it works by relocating keys if possible,
    and if a key cannot be relocated, it will be set as None
    :param dictionary:
    :param required:
    :param relocate_to:
    :return:
    """
    missing = required - dictionary.keys()
    misplaced = dictionary.keys() - required

    if missing or misplaced:
        target = dictionary
        flat = flatdict(dictionary)

        relocatable = set(filter(lambda x: x in flat.keys() and x not in target.keys(), missing))
        for k in relocatable:
            target[k] = flat[k]
        else:
            missing -= relocatable
            for k in missing:
                logging.warning("Missing required key: %s - setting to None" % k)
                target[k] = None

            misplaced = target.keys() - required
            for k in misplaced:
                logging.warning("Relocating %s to be a subkey of %s" % (k, relocate_to))
                if relocate_to not in dictionary:
                    target[relocate_to] = {}
                target[relocate_to][k] = target.pop(k)
            dictionary = target

    remove_duplicates(dictionary=dictionary, relocate_to=relocate_to)
    return target



def flatdict(d):
    """
    Flattens a dictionary
    :param d:
    :return:
    """
    def items():
        for k, v in d.items():
            if isinstance(v, dict):
                for sk, sv in flatdict(v).items():
                    yield sk, sv
            else:
                yield k, v
    return dict(items())


def suggest_normalizations(sample, threshold=1.0):
    """
    Attempts to identify spelling mistakes between two strings (a, b) using the Levenshtein distance metric, which is
    defined as the minimum edit distance between two strings. In order to identify candidates for replacement, we define
    a similarity measure (s) which is defined as such:

    f = jellyfish.levenshtein_distance
    s = floor([len(a)/f(a) + len(b)/f(a)]) / 2

    :param sample: a collection of terms to use
    :param threshold the threshold to use
    :return: a dict of candidates for normalization
    """
    if not all(map(lambda x: type(x) == list, [sample])):
        raise ValueError("normalize() expects scalar-valued arrays as input (e.g. a = [1, 2, 3])")
    else:
        seen = set()
        suggestions = []
        c = collections.Counter(sample).most_common()
        for t1, c1 in c:
            for t2, c2 in c:
                if t1 != t2 and (t1, t2) not in seen:
                    seen.add((t1, t2))
                    seen.add((t2, t1))

                    d = jellyfish.levenshtein_distance(t1, t2)
                    similarity = ((len(t1) / d) + (len(t2) / d)) // 2.0
                    if similarity > threshold:
                        suggestions.append(Suggestion(a=t1, b=t2, edit_distance=d))
        return suggestions


class Learning(unittest.TestCase):
    def test_autocorrect(self):
        metric = {'backend_version': '1.1.4',
         'input_hashes': [],
         'options': [],
         'probe_asn': 'AS7922',
         'probe_cc': 'US',
         'probe_city': None,
         'probe_ip': '127.0.0.1',
         'report_id': 'lsiL0V5w4mxk3LdvhOc0xbP8nLM0BtU9isdA4JL7thzk5hCrOtoo3k050iorx7Lx',
         'software_name': 'ooniprobe',
         'software_version': '1.3.2',
         'start_time': 1451992363.0,
         'test_helpers': {'backend': '111.111.111.111'},
         'test_keys': {'input': None,
                       'tampering': False,
                       'test_runtime': 10.249311923980713,
                       'test_start_time': 1451693514.0},
         'test_name': 'http_invalid_request_line',
         'test_version': '0.2'}

        missing = {'test_start_time', 'input', 'test_runtime', 'report_filename', 'data_format_version'}
        autocorrect(dictionary=metric, missing=missing, required=constants.schema)

    def test_normalize(self):
        valid = [
            'dns_consistency',
            'bridget',
            'http_requests',
            'http_host',
            'dns_spoof',
            'http_header_field_manipulation',
            'http_invalid_request_line',
            'tcp_connect',
            'multi_protocol_traceroute',
            'captive_portal',
            'bridge_reachability',
            'dns_injection',
            'lantern_circumvention_tool_test',
            'meek_frontend_requests_test',
            'psiphon_test',
            'openvpn',
        ]
        fake = [
            'dnsConsistency',
            'bridge_t',
            'brdgeReachabiliyt',
            'tcp_injection',
            'meek_front_requests',
            'mproto_traceroute',
        ]
        suggestions = suggest_normalizations(sample=valid + fake, threshold=1.0)

if __name__ == "__main__":
    unittest.main()