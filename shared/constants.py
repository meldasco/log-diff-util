output_folder = 'C:\\Users\\MDasco\\Projects\\_project-laboratrory\\sf-az-logs-utility\\output\\'

az_fname = output_folder + 'az_corrID_{0}.csv'
fe_fname = output_folder + 'sf_fe_corrID_{0}.csv'
ms_fname = output_folder + 'sf_ms_corrID_{0}.csv'

azdf = output_folder + 'azdf.csv'
sffe = output_folder + 'sffe.csv'
sfms = output_folder + 'sfms.csv'
path = output_folder + 'output.xlsx'

diff_assy = """
Name        : {0}
Difference  : {1}
Create Date : {2}
As of       : {3}
"""