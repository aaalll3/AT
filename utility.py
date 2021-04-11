

def read_apollo_stage1(read_from,wrtie_to):
    '''
    reading apollo origin code's output and 
    writing to a file
    '''
    w = open(wrtie_to,'w')
    with open(read_from,'r') as f:
        res = eval(f.read())
        for link,rel in res.items():
            w.write(f'{link[0]}|{link[1]}|{rel}\n')

def clear(self,path,out):
    f = open(path,'r')
    links = []
    link = None
    for line in f:
        line = line.strip()
        parts = line.split('|')
        if len(parts)<3:
            if link is None:
                link = [parts[0],parts[1]]
            else:
                link.append(parts[1])
                links.append(link)
                link = None
        else:
            links.append(parts)
    f.close()
    o = open(out,'w')
    for link in links:
        o.write('|'.join(link)+'\n')