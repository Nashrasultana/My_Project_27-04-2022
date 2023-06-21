import unittest
from ..dataflow_templates.null_primary_key_transform import NullPrimaryKeyTransform


### Testing PK not null ###
class NullPrimaryKeyTransformTests(unittest.TestCase):
    transform = NullPrimaryKeyTransform()

    def test_pk_not_null_no_null_values(self):
        element_pk_not_null = {"ID": "86922003", "DESC": 'THINGS MOB LTD CHINA ILE CMCC-SR1',
                               "COMPANY": 'THINGS MOB LTD', "MOB": '4G', "TYPE": 'Dongle', "SOMETHING": 'N', "SOMETHING_ELSE": 'N'}
        expected_output = {"ID": "86922003", "DESC": 'THINGS MOB LTD CHINA ILE CMCC-SR1',
                           "COMPANY": 'THINGS MOB LTD', "MOB": '4G', "TYPE": 'Dongle', "SOMETHING": 'N', "SOMETHING_ELSE": 'N'}
        actual_result = self.transform.pk_null_check(element_pk_not_null, "ID")
        assert expected_output == actual_result

    def test_pk_not_null_null_pk(self):
        element_pk_null =  {"ID": "", "DESC": 'THINGS MOB LTD CHINA ILE CMCC-SR1',
                               "COMPANY": 'THINGS MOB LTD', "MOB": '4G', "TYPE": 'Dongle', "SOMETHING": 'N', "SOMETHING_ELSE": 'N'}
        actual_result = self.transform.pk_null_check(element_pk_null, "ID")
        assert None == actual_result
