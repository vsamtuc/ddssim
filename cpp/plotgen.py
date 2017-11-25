from datavis import *

alist = [
    ("dset_name",str),
    ("dset_window",int),
    ("dset_warmup",int),
    ("dset_size",int),
    ("dset_duration",int),
    ("dset_streams",int),
    ("dset_hosts",int),
    ("dset_bytes",int),
    ("total_msg",int),
    ("total_bytes",int),
    ("traffic_pct",float),
    ("protocol",str),
    ("max_error",float),
    ("statevec_size",int),
    ("sites",int),
    ("sid",int),
    ("rounds",int),
    ("subrounds",int),
    ("sz_sent",int),
    ("total_rbl_size",int),
    ("bytes_get_drift",int)
]

attlist = [Attribute(name, atype) for name,atype in alist]
ds = Dataset()

commcost = ds.create_table('COMM', attlist)
ds.load_csv_in_table('COMM','sample_results.data')

#commcost1 = ds.create_view('COMMCOST1', 
#                      "SELECT *, epsilon/theta as eratio FROM COMMCOST")

commcost1 = ds.create_view('COMM1', 
                      "SELECT *, sites/max_error as keratio FROM COMM")


for Y in ['traffic_pct']:

    for sel in ds.axis_values(commcost, ['max_error', 'statevec_size']):
        plot = make_plot(commcost, 'sites', Y,
            axes=['protocol','max_error', 'statevec_size'],
            select={'max_error':sel[0], 'statevec_size':sel[1]},
            terminal=PNG()
            )
        plot.make()

    for sel in ds.axis_values(commcost, ['sites', 'statevec_size']):
        plot = make_plot(commcost, 'max_error', Y,
            axes=['protocol', 'sites', 'statevec_size'],
            select={'sites':sel[0], 'statevec_size':sel[1]},
            terminal=PNG()
            )
        plot.make()


    for sel in ds.axis_values(commcost, ['sites', 'max_error']):
        plot = make_plot(commcost, 'statevec_size', Y,
            axes=['protocol', 'sites', 'max_error'],
            select={'sites':sel[0], 'max_error':sel[1]},
            terminal=PNG()
            )
        plot.make()


for Y in ['traffic_pct']:

    for sel in ds.axis_values(commcost1, ['statevec_size']):
        plot = make_plot(commcost1, 'keratio', Y,
            axes=['protocol', 'statevec_size'],
            select={'statevec_size':sel[0]},
            terminal=PNG()
            )
        plot.make()

    plot = make_plot(commcost1, 'keratio', Y,
        axes=['protocol', 'statevec_size'],
        select={'statevec_size':greater_than(11000)},
        terminal=PNG()
    )
    plot.make()



#for Y in ['bytes_total', 'bytes_data', 'msg_data', 'msg_total']:
#    for sel in ds.axis_values(commcost1, ['theta', 'dset', 'servers']):
#
#       plot = make_plot(commcost1,'eratio', Y,
#                         axes=['depth','method','theta','dset','servers'],
#                         select={'theta':sel[0], 'dset':sel[1], 'servers':sel[2], 'depth':7},
#                         terminal=PNG())
#        plot.make()

