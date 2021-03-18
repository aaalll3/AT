import os

iso='/home/lwd/RIB.test/2020/12/isolario/Alderaan/rib.20201201.0000.read'
ripe='/home/lwd/RIB.test/2020/12/ripe/rrc00/bview.20201201.0000.read'
rv='/home/lwd/RIB.test/2020/12/routeviews/route-views.amsix/rib.20201201.0400.read'
with open(rv,'r') as f:
    for line in f:
        input(line)

