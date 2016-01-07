from pprint import  pprint
import collections
import jellyfish
import unittest

Suggestion = collections.namedtuple('Suggestion', ['a', 'b', 'edit_distance'])


def suggest_normalizations(sample, threshold):
    """
    Attempts to identify spelling mistakes between two strings (a, b) using the Levenshtein distance metric, which is
    defined as the minimum edit distance between two strings. In order to identify candidates for replacement, we define
    a similarity measure (s) which is defined as such:

    f = jellyfish.levenshtein_distance
    s = floor([len(a)/f(a) + len(b)/f(a)]) / 2

    :param sample: a collection of terms to use
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
        pprint(suggestions)

if __name__ == "__main__":
    unittest.main()