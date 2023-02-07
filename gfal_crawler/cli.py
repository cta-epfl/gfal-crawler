#!/usr/bin/env python
import hashlib
import os
import re
import time
import gfal2
import json
import optparse
import stat
import sys
from datetime import datetime


# adapted from https://gitlab.cern.ch/dmc/gfal2-bindings/-/blob/develop/example/python/gfal2_recursive_ls.py

class Crawler:

    def __init__(self, long=False,
                 max_levels=2, 
                 checksum_alg="ADLER32",
                 context=None):

        if context is None:
            self.context = gfal2.creat_context()
        else:
            self.context = context

        self.long = long        
        self.max_levels = max_levels
        self.checksum_alg = checksum_alg

    def _short_format(self, fname):
        return fname

    def _mode2str(self, mode):
        s = ['-'] * 10
        if stat.S_ISDIR(mode):
            s[0] = 'd'
        elif stat.S_ISLNK(mode):
            s[0] = 'l'
        elif not stat.S_ISREG(mode):
            s[0] = '?'

        for i in range(3):
            if mode & stat.S_IRUSR:
                s[1 + i * 3] = 'r'
            if mode & stat.S_IWUSR:
                s[2 + i * 3] = 'w'
            if mode & stat.S_IXUSR:
                s[3 + i * 3] = 'x'
            mode = mode << 3
        return ''.join(s)

    def _long_format(self, fname, fstat):
        return "%s %3d %5d %5d %10d %s %s" % \
            (self._mode2str(fstat.st_mode),
             fstat.st_nlink, fstat.st_uid, fstat.st_gid,
             fstat.st_size,
             datetime.fromtimestamp(fstat.st_mtime).strftime('%b %d %H:%M'),
             fname)


    def checksum_record(self, surl) -> dict:
        try:                
            record = {}
            if self.checksum_alg is None:
                checksum = None
            else:
                try:
                    checksum = self.context.checksum(surl, self.checksum_alg)
                    record[f'checksums'] = {self.checksum_alg: checksum}
                except gfal2.GError as e:
                    print("unable to compute checksum:", e, "!")
                    checksum = None
        except gfal2.GError as e:
            print("skip")
            
        return record



    def _crawl(self, url, out, level=0, harvest=None):
        if harvest is None:
            harvest = dict(
                size_so_far=0,
                files=[],
                errors=[],
                harvest_t0=time.time()
            )

        
        tabbing = '  ' * level
        
        print("opening", url)
        t0 = time.time()
    
        try:
            directory = self.context.opendir(url)
        except gfal2.GError as e:
            out.write(tabbing + "" + '!' + url)
            return harvest

        harvest['errors'].append({'url': url})

        print("opened in", time.time() - t0)
        
        entries = []     
        while True:
            try:
                (dirent, fstat) = directory.readpp()
            except gfal2.GError as e:
                out.write(tabbing + "" + '!' + url + ": " + repr(e) + "\n")
                continue

            if dirent is None or dirent.d_name is None or len(dirent.d_name) == 0:
                break

            entries.append((dirent, fstat))

        print("found entries", len(entries))

        for (dirent, fstat) in entries:

            # TODO: this is adopted from the example, to see if it's useful
            # dirent = directory.read()         
            # try:
            #     fstat = self.context.stat(full)
            # except gfal2.GError:                    
            #     fstat = self.context.st_stat()

            # if not self.params.all and dirent.d_name[0] == '.':
            #     continue

            
            # print(dir(self.context))

            surl = os.path.join(url, dirent.d_name)

            # TODO: this is adopted from the example, to see if it's useful
            # print(surl)

            # extra = list()
            # for xattr in []:
            #     extra.append(self.context.getxattr(surl, xattr))

            # self._print_ls_entry(dirent.d_name, st, extra)

            record = {
                'url': surl, 
                'fstat': {k.replace("st_", ""): getattr(fstat, k) for k in dir(fstat) if k.startswith("st_")}
            }

            record.update(self.checksum_record(surl))

            harvest['files'].append(record)

            basename = surl.split("/")[-1]

            harvest['size_so_far'] += fstat.st_size 

            crawl_rate = len(harvest['files'])/ (time.time() - harvest['harvest_t0'])

            out.write(f"{harvest['size_so_far']/1024/1024/1024:.2f} Gb in {len(harvest['files'])} files {crawl_rate:.1f} fps {tabbing} {self._long_format(basename, fstat)} {record.get('checksums', 'no checksums')}\n")
            
            if stat.S_ISDIR(fstat.st_mode) and level <= self.max_levels:
                self._crawl(surl, out, level + 1, harvest)
                


    
        return harvest


    def crawl(self, url, out=sys.stdout):
        return self._crawl(url, out)

def cli():
    parser = optparse.OptionParser()
    parser.add_option('-l', '--long', dest='long',
                      default=False, action='store_true',
                      help='Long format')
    parser.add_option('-m', '--max', dest='max_recursive',
                      default=1000, type='int',
                      help='Maximum recursive level')
    parser.add_option('-c', '--checksum-alg', dest='checksum_alg',
                      default=None, type='str',
                      help='checksum algorithm, e.g. ADLER32')

    (options, args) = parser.parse_args()
    if  len(args) != 1:
        parser.error('Incorrect number of arguments. Need a path')

    url = args[0]

    t0 = time.time()
    crawler = Crawler(options.long, options.max_recursive, options.checksum_alg)
    harvest = crawler.crawl(url, sys.stdout)

    
    tag = f"{re.sub('[^a-z0-9]+', '_', url.lower())}_{hashlib.md5(url.encode()).hexdigest()[:8]}_{datetime.fromtimestamp(t0).strftime('%Y%m%d_%H%M%S')}"

    json.dump({"harvest": harvest, "url": url, "start_timestamp": t0, "collected_in": time.time() - t0}, open(f"dcache_files_{tag}.json", "w"))


if __name__ == '__main__':
    cli()