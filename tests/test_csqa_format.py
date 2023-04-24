import json
from wikidata.client import Client as WDClient

from flags import WIKIDATA_REL_DICT_PATH, WIKIDATA_ENT_DICT_PATH
from transform_to_csqa_format import add_type_list_and_table_format


CLIENT = WDClient()
LOCAL_ENT_DICT = json.load(WIKIDATA_ENT_DICT_PATH.open())
LOCAL_REL_DICT = json.load(WIKIDATA_REL_DICT_PATH.open())


def test_type_list():
    # Test case data
    test_case = {
        "active_set": [
            "i(Q6726776,P136,Q182415)",
            "i(Q6726776,P495,Q884)"
        ],
        "expected_type_list": ["Q15961987", "Q3624078"]
    }

    add_type_list_and_table_format([test_case], CLIENT, LOCAL_ENT_DICT, LOCAL_REL_DICT)
    assert test_case["type_list"] == test_case[
        "expected_type_list"], f"Expected {test_case['expected_type_list']} but got {test_case['type_list']}"


def test_table_format():
    # Test case data
    test_case = {
        "active_set": [
            "i(Q4703598,P735,Q18916867)",
            "i(Q4703598,P69,Q4614)",
            "i(Q4703598,P27,Q30)",
            "i(Q4703598,P21,Q6581097)",
            "i(Q4703598,P106,Q42973)"
        ],
        "expected_table_format": [
            ["name", ["Al", "Boeke"]],
            ["given name", ["Al"]],
            ["educated at", ["University", "of", "Southern", "California"]],
            ["country of citizenship", ["United", "States", "of", "America"]],
            ["gender", ["male"]],
            ["occupation", ["architect"]]
        ]
    }

    add_type_list_and_table_format([test_case], CLIENT, LOCAL_ENT_DICT, LOCAL_REL_DICT)
    assert test_case["table_format"] == test_case[
        "expected_table_format"], f"Expected {test_case['expected_table_format']} but got {test_case['table_format']}"


if __name__ == "__main__":
    test_type_list()
    test_table_format()
    print("All tests passed.")
