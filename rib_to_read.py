import os
import requests
import random
import multiprocessing
from collections import defaultdict

def url_form(source,download_rc,year,month):
    if source == 'routeviews':
        url = "http://archive.routeviews.org/" + download_rc + '/bgpdata/' + year + '.' + month + '/RIBS/'
        hour = int(random.randint(0, 11) * 2)
        hour = str(hour).zfill(2)
        filename = "rib." + year + month + day + '.' + hour + '00.bz2'
        urlf = url + filename
    elif source == 'ripe':
        url = "http://data.ris.ripe.net/" + download_rc + '/' + year + '.' + month + '/'
        hour = int(random.randint(0, 2) * 8)
        hour = str(hour).zfill(2)
        filename = 'bview.' + year + month + day + '.' + hour + '00.gz'
        urlf = url + filename
    else:
        url = "https://www.isolario.it/Isolario_MRT_data/" + download_rc + '/' + year + '_' + month + '/'
        hour = int(random.randint(0, 11) * 2)
        hour = str(hour).zfill(2)
        filename = "rib." + year + month + day + '.' + hour + '00.bz2'
        urlf = url + filename
    return urlf

#从 routeviews，ripe，isolario 下载指定日期的 RIB 表
def download_rib(year, month, day, source, rc, download_dir):
    download_rc = rc
    if rc == 'route-views.oregon':
        download_rc = ''
    

    urlf = None
    filename = None
    if source == 'routeviews':
        url = "http://archive.routeviews.org/" + download_rc + '/bgpdata/' + year + '.' + month + '/RIBS/'
        hour = int(random.randint(0, 11) * 2)
        hour = str(hour).zfill(2)
        filename = "rib." + year + month + day + '.' + hour + '00.bz2'
        urlf = url + filename
    elif source == 'ripe':
        url = "http://data.ris.ripe.net/" + download_rc + '/' + year + '.' + month + '/'
        hour = int(random.randint(0, 2) * 8)
        hour = str(hour).zfill(2)
        filename = 'bview.' + year + month + day + '.' + hour + '00.gz'
        urlf = url + filename
    else:
        url = "https://www.isolario.it/Isolario_MRT_data/" + download_rc + '/' + year + '_' + month + '/'
        hour = int(random.randint(0, 11) * 2)
        hour = str(hour).zfill(2)
        filename = "rib." + year + month + day + '.' + hour + '00.bz2'
        urlf = url + filename
    
    try:
        r = requests.get(urlf, stream = True, verify=False)
        with open(download_dir + os.sep + filename, 'wb') as code:
            code.write(r.content)
        return download_dir + os.sep + filename
    except Exception as e:
        print(e)
        return None

#解压 RIB 表
def unzip_rib(filename):
    decompress_file = None
    if filename.endswith('.bz2'):
        print("Unzip bz2 file: ",filename)
        command = "bzip2 -d " + filename
        os.system(command)
        decompress_file = filename.replace('.bz2', '')
    elif filename.endswith('.gz'):
        print("Unzip gz file: ", filename)
        command = "gzip -d "+filename
        os.system(command)
        decompress_file = filename.replace('.gz', '')
    

    if os.path.exists(decompress_file):
        return decompress_file
    else:
        command = "rm " + filename
        os.system(command)
        return None

#利用 BGPdump 和 bgpreader 转换 RIB 文件为 ASCII 格式
def read_rib(source, filename):
    if not filename.endswith('.bz2') and not filename.endswith('.gz'):
        if source == 'isolario':
            print('Begin dump file: ', filename)
            command = "bgpdump -m " + filename + " > " + filename + ".dump"
            os.system(command)
            new_name = filename + ".dump"
        else:
            print('Begin read file: ', filename)
            command = "bgpreader -d singlefile -o rib-file=" + filename + " -m > " + filename + ".read"
            os.system(command)
            new_name = filename + ".read"
    
    command = "rm " + filename
    os.system(command)
    if os.path.exists(new_name) and os.path.getsize(new_name) / float(1024 * 1024) > 1:
        return True
    else:
        if os.path.exists(new_name):
            command = "rm " + new_name
            os.system(command)
        return False 

def checkdir(dname,func,piso):
    '''name should be a path object'''
    if type(dname)=='str':
        dname=os.path.join(os.path.abspath('..'),dname)
        print(f'[checkdir]in root dir {dname}')
    else:
        print(f'[checkdir]in {dname}')
    
    fnames = os.listdir(dname)
    for fname in fnames:
        iso=False
        if fname == 'isolario' or piso:
            iso=True
        name = os.path.join(dname,fname)
        if os.path.isdir(name):
            print(f'[checkdir]into {name}')
            checkdir(name,func,iso)
        else:
            print(f'[checkdir]find file {name}')
            func(name,iso)

def unzip(name,iso):
    decompress_file=None
    if name.endswith('.bz2'):
        print(f'[unzip]calling bzip2 to {name}')
        decompress_file = name.replace('.bz2', '')
        try:
            command = 'rm '+decompress_file
            os.system(command)
        except Exception as e:
            print(f'[unzip] {e}')
        try:
            command = "bzip2 -d " + name
            os.system(command)
        except Exception as e:
            print(f'[unzip] {e}')
    elif name.endswith('.gz'):
        print(f'[unzip]calling gzip to {name}')
        decompress_file = name.replace('.gz', '')
        try:
            command = 'rm '+decompress_file
            os.system(command)
        except Exception as e:
            print(f'[unzip] {e}')
        try:
            command = "gzip -d " + name
            os.system(command)
        except Exception as e:
            print(f'[unzip] {e}')
    else:
        print(f'[unzip]{name} not target')
        return

    if os.path.exists(decompress_file):
        print(f'[unzip]{name} succed')
    else:
        print(f'[unzip]{name} failed')

def read(filename, iso):
    if not filename.endswith('.bz2') and not filename.endswith('.gz'):
        if iso:
            print('[read]Begin dump file: ', filename)
            command = "bgpdump -m " + filename + " > " + filename + ".dump"
            os.system(command)
            new_name = filename + ".dump"
        else:
            print('[read]Begin read file: ', filename)
            command = "bgpreader -d singlefile -o rib-file=" + filename + " -m > " + filename + ".read"
            os.system(command)
            new_name = filename + ".read"


def delete(filename,iso):
    if not filename.endswith('.dump') and not filename.endswith('.read'):
        command = "rm "+filename
        os.system(command)

def form(filename):
    def get_address(filename):
        path = filename.split('/')
        address={}
        address['download_dir']=os.sep+os.path.join(path[1],path[2],path[3],path[4],path[5],path[6],path[7])
        address['year']=path[4]
        address['month']=path[5]
        address['source']=path[6]
        address['rc']=path[7]
        name=path[8]
        name=name.split('.')
        address['day']=name[1][6:]
        return address

    if filename.endswith('.bz2'):
        print(f'[form]bzip2 {filename}')
        return [True,get_address(filename)]
    elif filename.endswith('.gz'):
        print(f'[form]gzip {filename}')
        return [True,get_address(filename)]
    else:
        return [False,None]

def cdownload(filename,iso):
    result = form(filename)
    found=result[0]
    add=result[1]
    if found:
        command = 'rm '+filename
        download_rib(add['year'],add['month'],add['day'],add['source'],add['rc'],add['download_dir'])

#下载单个 RIB 表的线程
def worker(args):
    year = args[0]
    month = args[1]
    day = args[2]
    source = args[3]
    rc = args[4]
    download_dir = args[5]
    
    #下载某一天的 RIB 表可能会出现错误，因此需要尝试多次，try_time 即为尝试次数
    try_time = 10
    print('Begin dealing with ' + download_dir)
    success = False
    for i in range(try_time):
        download_file = download_rib(str(year), str(month).zfill(2), day, source, rc, download_dir)
        if download_file == None:
            continue
        else:
            success = True

        # decompress_file = unzip_rib(download_file)
        # if decompress_file == None:
        #     continue

        # if read_rib(source, decompress_file):
        #     success = True
        #     break
    if success:
        print('**Succesfully downloading with ' + download_dir)
    else:
        print('**Wrong downloading with ' + download_dir)

'''
    采用多线程的方式下载，解压 RIB 表，并利用 BGPDump，BGPReader 工具把解压后的文件转换为 ASCII 文件
    RIB 表的下载源为：
        1. routeviews
        2. ripe
        3. isolario
'''

downloaded = True
debug = False
if __name__ == "__main__" and downloaded:
    if debug:
        print('debug')
        # target=os.path.abspath('../RIB.test')
        # a=input(f'decompress to\n {target}')
        # checkdir(target,unzip,False)
        # a=input(f'read to\n {target}')
        # checkdir(target,read,False)
    else:
        tlist=[]
        dirnames=['routeviews','ripe','isolario']
        target=os.path.abspath('../RIB.test/2020/12/')
        for dn in dirnames:
            tlist.append([os.path.join(target,dn),delete,False])
        print(f'decompress to\n {target}')
        def ccheckdir(args):
            checkdir(args[0],args[1],args[2])

        with multiprocessing.Pool(3) as pool:
            pool.map(ccheckdir, tlist)
     
        
        #download

        # target=os.path.abspath('../RIB.test/2020/12/')
        # checkdir(target,cdownload,False)





if __name__ == "__main__" and not downloaded:
    source_rc = defaultdict(list)
    #routeviews route collector 列表
    source_rc['routeviews'] = ['route-views2.saopaulo', 'route-views3', 'route-views4', 
	'route-views6', 'route-views.chicago', 'route-views.chile', 
	'route-views.eqix', 'route-views.flix', 'route-views.isc', 
	'route-views.jinx', 'route-views.kixp', 'route-views.linx', 
	'route-views.napafrica', 'route-views.nwax', 'route-views.perth', 
	'route-views.saopaulo', 'route-views.sfmix', 'route-views.sg', 
	'route-views.soxrs', 'route-views.sydney', 'route-views.telxatl', 
	'route-views.wide', '', 'route-views.amsix',
	'route-views.phoix', 'route-views.mwix', 'route-views.rio',
	'route-views.fortaleza']
    #ripe route collector 列表
    for i in range(25):
        if i != 17:
            source_rc['ripe'].append('rrc' + str(i).zfill(2))
    #isolario route collector 列表
    source_rc['isolario'] = ['Alderaan', 'Dagobah', 'Korriban', 'Naboo', 'Taris']
    #RIB 表的下载目录
    main_dir = os.path.abspath('..') + os.sep + 'RIB'
    # print(f'downloading to {main_dir}')
    # while True:
    #     a=input('Y\\n\n')
    #     if a=='Y':
    #         break
    #     if a=='n':
    #         quit()

    #下载 RIB 表的起始时间
    begin_year = 2020
    begin_month = 12
    #下载 RIB 表的终止时间
    end_year = 2020
    end_month = 12
    #每个月下载的 RIB 表日期
    days_list = ['01','08', '15', '22']

    
    args_list = []
    #开始设置下载参数
    for year in range(begin_year, end_year + 1):
        month_begin = 1
        month_end = 12
        if year == begin_year:
            month_begin = begin_month
        if year == end_year:
            month_end = end_month
        
        for month in range(month_begin, month_end + 1):
            for day in days_list:
                for source, rc_list in source_rc.items():
                    for rc in rc_list:
                        if rc == '':
                            rc = 'route-views.oregon'
                        download_dir = os.sep.join((main_dir, str(year), str(month).zfill(2), source, rc))
                        if not os.path.exists(download_dir):
                            os.makedirs(download_dir)
                        
                        for file_name in os.listdir(download_dir):
                            full_name = os.sep.join((download_dir, file_name))
                            if os.path.isfile(full_name):
                                command = "rm " + full_name
                                os.system(command)
                        
                        args_list.append([year, month, day, source, rc, download_dir])
    print('\n'.join(str(arg) for arg in args_list))

    #以 6 线程的方式下载 RIB
    process_num = 6
    with multiprocessing.Pool(process_num) as pool:
        pool.map(worker, args_list)
     

        

