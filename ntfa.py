import csv
import sys
import os
import json

from flow_reader_config import FlowReaderConfig as fc
import aggregate as ag
from datetime import datetime,timedelta


def print_config():
    """Print Config"""
    with open(f'{fc.project_name}/config.json', 'r') as file:
        fc.config_data = json.load(file)
        print("Current Aggregator Config is:")
        for k, v in fc.config_data.items():
            match k:
                case "date_first_seen_index":
                    fc.date_first_seen_index=v
                case "duration_index":
                    fc.duration_index=v
                case "protocol_index":
                    fc.protocol_index=v
                case "source_ip_index":
                    fc.source_ip_index=v
                case "source_port_index":
                    fc.source_port_index=v
                case "number_packets_index":
                    fc.number_packets_index=v
                case "dest_ip_index":
                    fc.dest_ip_index=v
                case "dest_port_index":
                    fc.dest_port_index=v
                case "number_flows_index":
                    fc.number_flows_index=v
                case "label_index":
                    fc.label_index=v

            print(f"{k} : {v}")

def check_feature_index(feature_index,feature_name,feature_title_name):
    """Get feature index from the Json Or from the User"""
    if feature_name in fc.config_data:
        feature_index = fc.config_data[feature_name]
        print(f"The index for {feature_title_name} is {feature_index}")
        c = input("do you want to change it (y/n)?")
        if c.lower()=="y":
            feature_index = input(f"What is the index for {feature_title_name}?>")
    else:
        feature_index = input(f"What is the index for {feature_title_name}?")
    fc.config_data[feature_name] = feature_index
    return feature_index

def get_project_config():
    """Create a folder for the project and store in name in config"""
    fc.project_name = input("What is the project name?")
    try:
        
        os.makedirs(fc.project_name)
    except OSError:
        print(f"Failed to create folder '{fc.project_name}'")

def get_csv_file_config(filename):
    """Read CSV File"""
    change_config=False
    try:
        if(not os.path.exists(f'{fc.project_name}/config.json')):
            with open(f'{fc.project_name}/config.json', 'w') as file:
                json.dump(fc.config_data, file, indent=4)
                print("config.json created successfully")
            
        with open(f'{fc.project_name}/config.json', 'r') as file:
            fc.config_data = json.load(file)
            print("config.json loaded")
    except OSError as e:
            print("Can't load config.json")
            change_config = True
    
   
    with open(filename, newline='') as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader)
        print(f"The file has {len(header)} columns")
        header_index = 0
        for c in header:
            header_index +=1
            print(f"{header_index} - {c}")
        
        print_config()
        
        if((input("Do you want to change the config?(y/n)")).lower()=="y"):
            change_config = True
        
        if(change_config):
            print("\nTo aggregate the file, please enter the columns as per the displayed number(If your file doesn't have one of these columns leave the answer empty) :")
            fc.date_first_seen_index = check_feature_index(fc.date_first_seen_index,"date_first_seen_index","Date First Seen")
            fc.duration_index = check_feature_index(fc.duration_index,"duration_index","Duration")
            fc.protocol_index = check_feature_index(fc.protocol_index,"protocol_index","Protocol")
            fc.source_ip_index = check_feature_index(fc.source_ip_index,"source_ip_index","Source IP")
            fc.source_port_index = check_feature_index(fc.source_port_index,"source_port_index","Source Port")
            fc.number_packets_index = check_feature_index(fc.number_packets_index,"number_packets_index","Number Of Packets")
            fc.dest_ip_index = check_feature_index(fc.dest_ip_index,"dest_ip_index","Destination IP")
            fc.dest_port_index = check_feature_index(fc.dest_port_index,"dest_port_index","Destination Port")
            fc.number_flows_index = check_feature_index(fc.number_flows_index,"number_flows_index","Number Of Flows")
            fc.label_index = check_feature_index(fc.label_index,"label_index","Label Index")
        
            # print("\n")
            
    
            with open(f'{fc.project_name}/config.json', 'w') as file:
                json.dump(fc.config_data, file, indent=4)
                print("config.json stored successfully")

        fc.window_size = input("What is the window in sec?")
        return fc

def get_label(label_list):
    if(label_list and len(label_list)>0):
        return label_list[0]
    return ""

def get_sum_duration(duration_list):
    r = 0.0
    for d in duration_list:
        try:
            r = r + float(d)
        except ValueError:
            pass
    return r

def get_sum_number_packets(packets):
    r = 0
    for p in packets:
        try:
            r = r + int(p)
        except ValueError:
            pass
    return r

def final_output(frames):
    r = []
    file = open("output.csv", "w")
    title="start_frame,end_frame,src_ip,duration,protocol,src_port,dst_ip,dst_port,number_packets,label" + "\n"  
    file.write(title)
    for f in frames:
        s = ""
        for k in f.keys():    
                i = f[k]
                s = s + i["start_frame"].isoformat() + ","
                s = s + i["end_frame"].isoformat() + ","
                s = s + k + ","
                duration = get_sum_duration(i["duration"])
                s = s + str(round(duration,3)) + ","
                s = s + str(len(i["protocol"])) + ","
                s = s + str(len(i["src_port"])) + ","
                s = s + str(len(i["dst_ip"])) + ","
                s = s + str(len(i["dst_port"])) + ","
                s = s + str(get_sum_number_packets(i["number_packets"])) + ","
                s = s + get_label(i["label"]) + "\n"
        
        file.write(s)

def aggregate(filename,fc):
     """Aggregate  File"""
     with open(filename, newline='') as csv_file:
         reader = csv.reader(csv_file)
         header = next(reader)

         i = 0

         first_row = next(reader)
         start_frame =datetime.fromisoformat(first_row[int(fc.date_first_seen_index)-1])
         end_frame = datetime.fromisoformat(first_row[int(fc.date_first_seen_index)-1])+ timedelta(seconds=int(fc.window_size))
         frame={}
         frames=[]
         for row in reader:
            #Rest frame
            if datetime.fromisoformat(row[int(fc.date_first_seen_index)-1])>end_frame:
                frames.append(frame)
                frame={}
                end_frame = datetime.fromisoformat(row[int(fc.date_first_seen_index)-1])+ timedelta(seconds=int(fc.window_size))
                start_frame =datetime.fromisoformat(row[int(fc.date_first_seen_index)-1])
                print(start_frame,end_frame )

            src_ip = row[int(fc.source_ip_index)-1]
       
            if  src_ip not in frame:
                frame[src_ip]={"start_frame":start_frame,"end_frame":end_frame, "duration":[],"protocol":[],"src_port":[],"dst_ip":[],"dst_port":[],"label":[],"number_packets":[]}
            ip_info = frame[src_ip]
            
            duration = row[int(fc.duration_index)-1]
            ip_info["duration"].append(duration)

            protocol = row[int(fc.protocol_index)-1]
            if protocol.strip() not in ip_info["protocol"]:
                ip_info["protocol"].append(protocol.strip())

            src_port = row[int(fc.source_port_index)-1]
            if src_port not in ip_info["src_port"]:
                ip_info["src_port"].append(src_port)
            
            dst_ip = row[int(fc.dest_ip_index)-1]
            if dst_ip not in ip_info["dst_ip"]:
                ip_info["dst_ip"].append(dst_ip)
            
            dst_port = row[int(fc.dest_port_index)-1]
            if dst_port not in ip_info["dst_port"]:
                ip_info["dst_port"].append(dst_port)

            number_packets = row[int(fc.number_packets_index)-1]
            ip_info["number_packets"].append(number_packets)
                
            label = row[int(fc.label_index)-1]
            label =label  +"_" + row[int(fc.label_index)-2]
            if label not in ip_info["label"]:
                ip_info["label"].append(label)


         final_output(frames)


def main(argv):
    
    get_project_config()
    fc = get_csv_file_config(argv[0])
    print(fc)
    aggregate(argv[0],fc)
   

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Error: No flow data provided")
        print("Usage: python ntfa.py <file.csv>")
        sys.exit(1)
    main(sys.argv[1:])
