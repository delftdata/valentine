from ast import literal_eval

s = "(('Prospect_approx_columns_test_source',_'AgencyID'),_('Prospect_approx_columns_test_source',_'AgencyID'))"


print(type(literal_eval(s.replace(",_", ","))[0]))
