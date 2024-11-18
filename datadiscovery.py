import pandas as pd
import _env
import _core
import _common
import datadiscovery_sql as _sql
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from snowflake.connector.pandas_tools import write_pandas

#Ensure the table name (context), idco  l and fetchcol are in correct case in the config file as required by MySql
'''
SQLAclchemy converts the column nmaes to lower case so to reconstruct DMLs actual cases are used 
but for referring/comparing column names lower cases are used    
'''
orgfilepathname = "C:\\Users\\SreedharanSrinivasan\\OneDrive - CORL Technologies\\Desktop\\Integration\\Configs\\org_discovery.csv"
metafilepathname = "C:\\Users\\SreedharanSrinivasan\\OneDrive - CORL Technologies\\Desktop\\Integration\\Configs\\data_discovery_sql.csv"
runenv = 'prod'
srcenv = 'prod'
connector = 'DATA_DISCOVERY'
sf_src_db = 'raw_portal_prod' 
ms_src_db = 'src_portal2_prod'
#tgt_db = 'sf_portal2_prod'
logenv = 'local'
profilename = _env.AWS_PROFILE[runenv]
sf_schema_tag = sf_src_db
ms_host_tag = srcenv
ms_db_tag = ms_src_db

orgtbl = 'Org'
datatypedict = {'int': 'np.int64', 'string': 'string'}

dataoutputtxt = {
    'context': 'Analyzing table: ',
    'ids_str': 'List of Ids are: ',
    'ids_wo_min_str': 'List of Ids excluding minimum id: ',
    'id_tgt_str': 'Target id is :',
    'tgt_id_cnt': 'Target Vendor count is: ',
    'id_max_cnt': 'Id with Max cust is: ',
    'max_cnt': 'Max count is: ',
    'missing_id_lst': 'Ids that are not associated with the target vendor org id are: ',
    'records_non_tgt': 'Total records not associated with the target vendor org is: '
}
#Dictionary to store the output
dataoutput = {}

def get_org_list(env):
    envname = srcenv
    #metafilepathname = _core.get_metadatafile(env, connector, True)
    df_orglst = pd.read_csv(orgfilepathname, sep=',')
    df_orglst['domain_str'] = df_orglst['domain_str'].fillna("None").astype(str)
    df_orglst = df_orglst.astype({'org_str':str, 'domain_str':str})
    df_orglst['target_orgid'] = df_orglst['target_orgid'].fillna(0).astype(int)
    df_org_tmp = df_orglst[(df_orglst['status'] == 'active')]
    return df_org_tmp

def get_metadata_ms(env):
    envname = srcenv
    #metafilepathname = _core.get_metadatafile(env, connector, True)
    df_meta = pd.read_csv(metafilepathname, sep=',', index_col='context', converters={'fetch_col': str})
    df_meta_tmp = df_meta[(df_meta['status'] == 'active') & \
                          (df_meta['environment_name'] == envname)]
    #df_meta_tmp['sql'] = df_meta_tmp['sql'].str.replace('raw.portal_prod.', '')
    return df_meta_tmp

def create_dml(context, param=None):
    dml_str_tmp = _sql.DATA_DISCOVERY_SQL[context].format(param)
    dml_str = f"{dml_str_tmp}"
    return dml_str

'''
Runs through the Org table for the provided orgname string
Groups the OrgIds by the root domain name and takes a count
The root domain name with maximum count is considered for the duplicate analysis
It is difficult to ascertain the other domain names belong to the same comapny withour further rules (at this time)
'''
def org_run(row_context, idcol, df_main, tgt):
    idcolname = idcol.lower()
    df_main.set_index(idcolname, inplace=True)
    df_org_grp = df_main.groupby('rootname').size().sort_values()
    df_tgtorg = df_main.query('rootname == @df_org_grp.idxmax()')
    orgid_lst = df_tgtorg.index.to_list()
    orgid_lst.sort()
    orgid_str = ', '.join(map(str,orgid_lst))
    # Take the lowest orgid as the target id
    # The following logic is based on the rationale that the org_lst is already sorted
    # Using the list and pop method to keep the code simple. 
    # A more robust way is documented and commented below this code as well
    if tgt == 0:
        tgt_orgid = orgid_lst.pop(0)
    else: 
        tgt_orgid = tgt
        orgid_lst.remove(tgt)
    orgid_wo_min_lst = orgid_lst
    orgid_wo_min_str = ', '.join(map(str,orgid_wo_min_lst))
    '''
    #Alternate way to create the target org id and the list without it
    orgid_wo_min_lst = df_tgtorg.query('orgrank > 1').index.to_list()
    orgid_wo_min_str = ', '.join(map(str,orgid_wo_min_lst))
    tgt_orgid = df_tgtorg.query('orgrank == 1').index[0]
    '''
    dataoutput_main = {}
    dataoutput_main = {
        'id_col': idcol,
        'ids_lst': orgid_lst, 
        'ids_str': orgid_str,
        'ids_wo_min_lst': orgid_wo_min_lst,
        'ids_wo_min_str': orgid_wo_min_str,
        'id_tgt_str': tgt_orgid,
        'records_non_tgt': len(orgid_wo_min_lst)
        }
    #print(dataoutput_main)
    del df_main, df_org_grp, df_tgtorg
    return dataoutput_main

def select_run(row_context, idcol, tgt_orgid, df_select):
    idcolname = idcol.lower()
    ids_lst = df_select[idcolname].drop_duplicates().sort_values().to_list()
    qrystr = f'{idcolname} != @tgt_orgid'
    records_non_tgt = df_select.query(qrystr).shape[0]
    if len(ids_lst) == 0: ids_str = 'No records found'
    else: ids_str = ', '.join(map(str,ids_lst))
    dataoutput_select = {}
    dataoutput_select = {
        'id_col': idcol,
        'ids_lst': ids_lst, 
        'ids_str': ids_str,
        'records_non_tgt': records_non_tgt
        }
    return dataoutput_select

def missing_run(row_context, idcol, fetchcol, tgt_orgid, orgid_wo_min_lst, df_missing):
    idcolname = idcol.lower()
    fetchcolname = fetchcol.lower()
    if not df_missing.empty:
        df_grp = df_missing.groupby(idcolname).size()
        try:
            tgt_id_cnt = df_grp[tgt_orgid]
        except KeyError as err: 
            print(f'No data for key {err} in {row_context}')
            tgt_id_cnt = 0
        except Exception as err: 
            print(f'Error while running {row_context}: msg: {type(err)} as {err}')
            tgt_id_cnt = 0

        id_max_cnt = df_grp.idxmax()
        max_cnt = df_grp.max()
        tgtid_qry_str = f'{idcolname} == @tgt_orgid'
        tgtid_ids_lst = df_missing.query(tgtid_qry_str)[fetchcolname].\
                drop_duplicates().sort_values().to_list()
        #List of customer orgids that are not associated with target vendor id but with other vendor ids
        non_tgtid_qry_str = f'{idcolname} in @orgid_wo_min_lst and {fetchcolname} not in @tgtid_ids_lst'
        non_tgtid_ids_lst = df_missing.query(non_tgtid_qry_str)[fetchcolname].\
                    drop_duplicates().sort_values().to_list()
        if df_missing.dtypes[fetchcolname] == 'string':
            non_tgtid_ids_str = "'" + "', '".join(non_tgtid_ids_lst) + "'"
        else: non_tgtid_ids_str = ", ".join(map(str,non_tgtid_ids_lst))
        record_cnt_qry_str = f'{idcolname} != @tgt_orgid'
        records_non_tgt = df_missing.query(record_cnt_qry_str).shape[0]
    else:
        tgt_id_cnt = None
        id_max_cnt = None
        max_cnt = None
        non_tgtid_ids_lst = []
        non_tgtid_ids_str = None,
        records_non_tgt = 0
    dataoutput_missing = {}
    dataoutput_missing = {
        'id_col': idcol,
        'fetch_col': fetchcol,
        'tgt_id': tgt_orgid, 
        'tgt_id_cnt': tgt_id_cnt,
        'id_max_cnt': id_max_cnt,
        'max_cnt': max_cnt,
        'missing_id_lst': non_tgtid_ids_lst,
        'missing_id_str': non_tgtid_ids_str,
        'records_non_tgt': records_non_tgt
        }
    return dataoutput_missing

def duplicate_orgs(orgstr, tgt=0, domainstr=''):
    org_name = orgstr.lower()
    df_active = get_metadata_ms(runenv)
    
    ms_connparams = _core.get_msparams(ms_host_tag, ms_db_tag)
    ms_engine = _core.get_msconn_alchemy(ms_connparams)
    #msconn = msengine.connect()
    sf_connparams = _core.get_sfparams(sf_schema_tag)
    sf_tgt_schema = _core.get_sf_schema(sf_schema_tag, 0, 0)
    #Using SQLAlchemy engine for reading but regular connection for writing

    #run_dt = datetime.now().strftime('%Y-%m-%dT06:00:00Z')
    logfilename = _core.get_logfilename(logenv, f'VIEWS_{connector}', runenv, org_name)
    logfile = open(logfilename, "x")
    print(f'Starting the run for: {org_name}\n', file=logfile)
    sf_engine = _core.get_sfconn_alchemy(sf_connparams)
    dataoutput = {org_name: {}}
    #Uncomment If using snowflake as source
    with sf_engine.connect() as tbl_conn:
    #with ms_engine.connect() as tbl_conn:
        for index, row in df_active.iterrows():
            row_context = index
            row_run_mode = row['run_mode'].lower()
            row_operation = row['operation'].lower()
            idcol = row['id_col']
            fetchcol = row['fetch_col']
            row_fetchcoltype = row['fetch_col_type']
            fetchcoltype = row_fetchcoltype
            dql_qry_str = ''
            df_row = pd.DataFrame()
            org_dict = dataoutput.get(org_name)
            if org_dict is not None:
                org_context_dict = org_dict.get(orgtbl)
                if org_context_dict is not None:
                    orgid_str = org_context_dict.get('ids_str')
                    tgt_orgid_str = org_context_dict.get('id_tgt_str')
                    orgid_wo_min_lst = org_context_dict.get('ids_wo_min_lst')
                else: orgid_str = 0
            else: orgid_str = 0
            print(f'Running: {row_context}, {row_run_mode}, {idcol}, {fetchcol}, {orgid_str}')
            dataoutput[org_name][row_context] = {}
            dataoutput[org_name][row_context]['context'] = row_context
            dataoutput[org_name][row_context]['run_mode'] = row_run_mode
            dataoutput[org_name][row_context]['operation'] = row_operation
            if row_run_mode == 'main':
                dql_qry_str = df_active.loc[row_context, 'sql'].format(org_name,domainstr)
                #print(dql_qry_str)
                df_row = pd.read_sql_query(dql_qry_str, tbl_conn)
                org_dict = org_run(row_context, idcol, df_row, tgt)
                dataoutput[org_name][row_context].update(org_dict)
                #print(dataoutput[org_name][row_context])
            elif row_run_mode == 'select':
                dql_qry_str = df_active.loc[row_context, 'sql'].format(orgid_str)
                df_row = pd.read_sql_query(dql_qry_str, tbl_conn)
                select_dict = select_run(row_context, idcol, tgt_orgid_str, df_row)
                dataoutput[org_name][row_context].update(select_dict)
                #print(dataoutput[org_name][row_context])
            elif row_run_mode == 'missing':
                dql_qry_str = df_active.loc[row_context, 'sql'].format(orgid_str)
                df_row = pd.read_sql_query(dql_qry_str, tbl_conn, dtype={fetchcol.lower(): fetchcoltype})
                missing_dict = missing_run(row_context, idcol, fetchcol, \
                                tgt_orgid_str, orgid_wo_min_lst, df_row)
                dataoutput[org_name][row_context].update(missing_dict)
            dataoutput[org_name][row_context]['query'] = dql_qry_str
    #print(dataoutput)
    for tbl_name in dataoutput[org_name]:
        out_dict = dataoutput[org_name][tbl_name]
        for key in out_dict:
            txt = dataoutputtxt.get(key)
            value = out_dict[key]
            if txt is not None:
                print(f'{txt} {value}', file=logfile)
        print(f"{'':-^30}", file=logfile)
    #print(f"{'':-^30}", file=logfile)
    print(f"{'Completed database queries':=^50}", file=logfile)
    dml_dict = generate_dml(org_name, dataoutput)
    dml_dict_len = len(dml_dict[org_name])
    for id in range(dml_dict_len, 0, -1):
        tbl_name = dml_dict[org_name][id]['table']
        dql_select = dml_dict[org_name][id].get('select', '-- None')
        dml_update = dml_dict[org_name][id].get('update', '-- No updates required')
        dml_delete = dml_dict[org_name][id].get('delete', '-- No deletes required')
        #Adding a double dash so the text appears as comment in SQLEditor when copied to one
        print(f'-- DML for {tbl_name}: \n{dql_select};\n{dml_update};\n{dml_delete};', file=logfile)
        print(f"-- {'':-^30}", file=logfile)
    sf_engine.dispose()
    ms_engine.dispose()
    logfile.close()
    
def generate_dml(org_name, dataoutput):
    dml_dict = {}
    dml_dict[org_name] = {}
    del_lst = dataoutput[org_name][orgtbl]['ids_wo_min_str']
    tgt_id = dataoutput[org_name][orgtbl]['id_tgt_str']
    sequence = 0
    for tbl_name in dataoutput[org_name]:
        sequence += 1
        dml_dict[org_name][sequence] = {}
        dml_dict[org_name][sequence]['table'] = tbl_name
        out_dict = dataoutput[org_name][tbl_name]
        id_col = out_dict.get('id_col')
        records_non_tgt = out_dict.get('records_non_tgt')
        select_qry_tmp = out_dict.get('query')
        select_qry = select_qry_tmp.replace("raw.portal_prod.", "")
        dml_dict[org_name][sequence]['select'] =  select_qry
        if records_non_tgt == 0: continue
        if out_dict.get('operation') == 'update':
            update_qry =  f'Update {tbl_name} Set {id_col} = {tgt_id} Where {id_col} in ({del_lst})'
            dml_dict[org_name][sequence]['update'] =  update_qry
        elif out_dict.get('operation') == 'merge': 
            fetch_col = out_dict.get('fetch_col')
            update_lst = out_dict.get('missing_id_str')
            update_qry =  f'Update {tbl_name} Set {id_col} = {tgt_id} Where {id_col} in ({del_lst})'
            if update_lst != '':
                update_qry += f' and {fetch_col} in ({update_lst})'
                dml_dict[org_name][sequence]['update'] = update_qry
            delete_qry = f'Delete From {tbl_name} Where {id_col} in ({del_lst})'
            #print(dml_dict[org_name][tbl_name])
            dml_dict[org_name][sequence]['delete'] =  delete_qry
        else:
            delete_qry = f'Delete From {tbl_name} Where {id_col} in ({del_lst})'
            dml_dict[org_name][sequence]['delete'] =  delete_qry
    return dml_dict

def discover_orgs():
    df_org = get_org_list(runenv)
    for index, row in df_org.iterrows():
        orgstr = row['org_str']
        domainstr = row['domain_str']
        tgtid = row['target_orgid']
        duplicate_orgs(orgstr, tgtid, domainstr)

def main(args=None):
        discover_orgs()

if __name__ == "__main__":
    args = _core.sys.argv[1:] 
    if not args:
        main()
    else:
        main(args)