#Create a Building inventory csv file from building inventory and addresspoint
#Create a shapefile from qgis -- This part done in QGIS
#Upload shapefile to incore data service

import pandas as pd
from pyincore import IncoreClient, DataService, SpaceService, Dataset

#Import input data
all_buildings = pd.read_csv("../incore_data/MMSA Building Inventory/all_bldgs_ver5_Project.csv")
add_point = pd.read_csv("../incore_data/MMSA Address Point Inventory/IN-CORE_2dv3_AddressPointInventory_2020-01-22_strctid.csv")

print("Building inventory iported, length - {}".format(len(all_buildings)))
print("Address point inv inported, length - {} \n".format(len(add_point)))

#Clean address point
ap = add_point.loc[:, ['guid', 'blockid','blockidstr', 'geometry', 'huestimate']]
ap.drop_duplicates(inplace=True)

#Merge to create building inventory
bldg_inventory = pd.merge(all_buildings, ap, on=['guid'])
bldg_inventory["Latitude"] = [float(x.split(" ")[1][1:]) for x in bldg_inventory.geometry]
bldg_inventory["Longitude"] = [float(x.split(" ")[2][:-1]) for x in bldg_inventory.geometry]
bldg_inventory.to_csv("../derived_data/MMSA_Building_Inventory.csv")


print("Building inventory Created")
print("---------------------------")
print("Length - {}".format(len(bldg_inventory)))
print("unique guids - {} \n".format(len(bldg_inventory.guid.unique())))


shp_file = input("Is shape file created?[y/n]: ")

if shp_file=='y':
    shp_loc = input("shapefile package location + filename without extension ")
    print("Including Files - \n ")
    print(shp_loc+'.shp')
    print(shp_loc+'.shx')
    print(shp_loc+'.prj')
    print(shp_loc+'.dbf')
    proceed = input("\n Ready to upload to incore. Proceed?[y/n] ")
    
    if proceed=='y':
        print("\n Uploading dataset to incore")
        print("================================")
        client = IncoreClient()
        data_services = DataService(client)
        space_services = SpaceService(client)

        dataset_metadata = {
        "title":"MMSA All Building Inventory",
        "description": "Shelby building inventory containing strctid, longitude, latitude and block id",
        "dataType": "ergo:buildingInventoryVer5",
        "format": "shapefile"
        }

        created_dataset = data_services.create_dataset(dataset_metadata)
        dataset_id = created_dataset['id']
        print('dataset is created with id ' + dataset_id)

        files = [shp_loc+'.shp', shp_loc+'.shx', shp_loc+'.prj', shp_loc+'.dbf']
        full_dataset = data_services.add_files_to_dataset(dataset_id, files)
        
        print("Data upload complete - dataset summary \n")
        print(full_dataset)
        
        print("Checking to see if it worked by loading this from remote dataset... \n")
        
        buildings = Dataset.from_data_service(dataset_id, data_services)
        print(buildings)


        

