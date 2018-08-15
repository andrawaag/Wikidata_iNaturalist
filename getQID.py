import pandas as pd
import numpy as np
from wikidataintegrator import wdi_core, wdi_login, wdi_property_store, wdi_helpers
from wikidataintegrator.ref_handlers import update_retrieved_if_new_multiple_refs
from wikidataintegrator.wdi_helpers import try_write
import os

df = pd.read_csv('inaturalist_taxon_mapping.csv', dtype=object)

# Set bot credentials
try:
    from scheduled_bots.local import WDUSER, WDPASS
except ImportError:
    if "WDUSER" in os.environ and "WDPASS" in os.environ:
        WDUSER = os.environ['WDUSER']
        WDPASS = os.environ['WDPASS']
    else:
        raise ValueError("WDUSER and WDPASS must be specified in local.py or as environment variables")

# separate the table into separate dataframes per species
taxon_sources = dict()
for source in df.ext_source.unique():
    taxon_sources[source] =  df.loc[df["ext_source"]==source]

# Process the a specific data source in this case CONABIO
print(len(taxon_sources["CONABIO"]))

PROPS = {
    'GBIF': 'P846',
    'Calflora': 'P3420',
    'CONABIO': 'P4902',
    'iNaturalist taxon ID': "P3151"
}

wdi_property_store.wd_properties['P3151'] = {
    'datatype': 'string',
    'name': 'iNaturalist taxon ID',
    'domain': ['taxons'],
    'core_id': True
}

wdi_property_store.wd_properties['P4902'] = {
    'datatype': 'string',
    'name': 'CONABIO',
    'domain': ['taxons'],
    'core_id': True
}

i = 0
login = wdi_login.WDLogin(WDUSER, WDPASS)
write=True
for index, row in taxon_sources["CONABIO"].iterrows():
    prep = dict()
    prep["P3151"] = [wdi_core.WDString(str(row.taxon_id), prop_nr='P3151')]
    prep["P4902"] = [wdi_core.WDString(str(row.taxon_id), prop_nr='P4902')]
    print(row.taxon_id)
    print(row.source_identifier)

    data2add = []
    for key in prep.keys():
        for statement in prep[key]:
            data2add.append(statement)
            print(statement.prop_nr, statement.value)

    wdPage = wdi_core.WDItemEngine(data=data2add, domain="taxons", item_name=row.inat_name)
    print(wdPage.get_wd_json_representation())
    try_write(wdPage, record_id=row.taxon_id, record_prop=PROPS["iNaturalist taxon ID"],
              edit_summary="Updated a iNaturalistTaxon", login=login, write=write)
    i=+1
    if i>3:
        break



