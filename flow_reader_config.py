class FlowReaderConfig:
    """ Store the project and flow information """
    project_name=""
    config_data={}
    date_first_seen_index=-1
    duration_index=-1
    protocol_index=-1
    source_ip_index=-1
    source_port_index=-1
    number_packets_index=-1
    dest_ip_index=-1
    dest_port_index=-1
    number_flows_index=-1
    window_size=-1
    label_index=-1
    # def __init__(
    #     self
        # date_first_seen_index,
        # duration_index,protocol_index, 
        # source_ip_index,source_port_index,
        # dest_ip_index,dest_port_index,
        # number_packets_index,number_flows_index
       # ):
            # self.date_first_seen_index = date_first_seen_index
            # self.duration_index = duration_index
            # self.protocol_index = protocol_index
            # self.source_ip_index = source_ip_index
            # self.source_port_index = source_port_index
            # self.number_packets_index = number_packets_index
