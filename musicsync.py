
import sys
import os
import stat
import errno
import time
import math
from fcntl import *
import subprocess
from subprocess import Popen, PIPE
import json
from collections import namedtuple
import xml.etree.ElementTree
import urllib.parse
import threading
from queue import Queue
import multiprocessing

import mutagen.easymp4
import mutagen.easyid3
import mutagen.oggopus
import mutagen.flac

fileinfo = namedtuple('fileinfo', ['relpath', 'stat', 'duration', 'bitrate'])

TMPDIR          = '/tmp'
RHYTHMBOXDB     = os.path.expanduser('~/.local/share/rhythmbox/rhythmdb.xml')
LOSSYFORMATS    = {'.mp3', '.m4a', '.ogg', '.oga', '.wma', '.mpc', '.opus'}
LOSSLESSFORMATS = {'.flac', '.wav'}
MUSICFORMATS    = LOSSYFORMATS | LOSSLESSFORMATS
# see http://www.jukefox.org/index.php/faq
COVERS          = {'cover.jpg', 'albumart.jpg', 'folder.jpg', 'cover.png', 'albumart.png', 'folder.png'}
# skip these files
OTHERFORMATS    = {'.part', '.swp', '.txt', '.jpg', '.png', '.bmp', '.gif', '.zip', '.rar'}
IGNORE_FILE     = 'musicsync-ignore.txt'

# http://www.hydrogenaudio.org/forums/index.php?showtopic=44310
# use ~66kbps.
# previously, this was '0.30', giving ~85kbps.
# ~50kbps is the treshold for transparency for me, so ~66kbps should be enough
# for most circumstances (giving very good quality for a relatively low bitrate).
AAC_QUALITY  = '0.25'
OPUS_QUALITY = '65'
LOSSY_EXT = '.m4a'
MINIMUM_TRANSCODE_BITRATE = 320 # highest
MAXPROCS = multiprocessing.cpu_count()

tmp_number = 0

class MusicSync:
    ''' Copies new files from one source to a destination, possibly transcoding
        them (at least when they are lossless). Removes all files that aren't in
        the source. Updates files changed at one of the two places.
    '''
    def __init__ (self, source, dest, exclude=(), excludeTranscode=(), lossy_ext=LOSSY_EXT, minimum_transcode_bitrate=MINIMUM_TRANSCODE_BITRATE, confirmRemove=True):
        self.source = source.rstrip('/')+'/'
        self.dest = dest.rstrip('/')+'/'
        self.exclude = exclude
        self.excludeTranscode = excludeTranscode
        self.lossy_ext = lossy_ext
        self.minimum_transcode_bitrate = minimum_transcode_bitrate
        self.confirmRemove = confirmRemove
        self.fileDb = None
        self.artistDb = None

    def sync(self):
        self.musicDirs = {} # directories containing music

        # mapping of trackpath: full (source) file name
        self.seenFiles = {}

        self.scandir(self.source)

        self.doSync()
        self.convertLossless()
        self.transcodeLossy()

        self.findOld()
        self.mayClearOld()

    def scandir(self, base):
        ''' Scan source directories '''

        for directory, dirs, files in os.walk(base):
            if 'nophone' in dirs:
                dirs.remove('nophone')
            if 'EAC' in dirs:
                dirs.remove('EAC')
            if '.sync' in dirs:
                dirs.remove('.sync')
            if IGNORE_FILE in files:
                for fn in open(os.path.join(directory, IGNORE_FILE), 'r').readlines():
                    fn = fn.rstrip('\r\n')
                    if not fn:
                        continue
                    if fn in dirs:
                        dirs.remove(fn)
                    elif fn in files:
                        files.remove(fn)
                    else:
                        print ('Ignored filename not found:', fn)
            dirs.sort()
            files.sort()
            for fn in files:
                path = os.path.join(directory, fn)

                if not self.mayCopy(path):
                    continue

                if path.find('/.unison.') >= 0:
                    continue

                ext = os.path.splitext(path)[1].lower()
                if ext not in MUSICFORMATS and fn.lower() not in COVERS:
                    continue

                relpath = os.path.relpath(path, base)
                reldir  = os.path.dirname(relpath)
                trackpath = os.path.splitext(relpath)[0]

                if self.addSeen(trackpath, path):
                    continue

                fulldir = os.path.join(base, reldir)
                if reldir in self.musicDirs:
                    if self.musicDirs[reldir] != fulldir:
                        print ('Duplicate dir!')
                        print ('dir 1:', self.musicDirs[reldir])
                        print ('dir 2:', fulldir)
                        # get rid of this warning
                        #self.musicDirs[reldir] = fulldir
                        continue
                else:
                    if ext in MUSICFORMATS:
                        self.musicDirs[reldir] = fulldir

    def getArtistDB(self):
        if self.artistDb is None:
            self.loadDB()
        return self.artistDb

    def getFileDB(self):
        if self.fileDb is None:
            self.loadDB()
        return self.fileDb

    def loadDB(self):
        self.artistDb = {}
        self.fileDb = {}
        xmldata = xml.etree.ElementTree.parse(RHYTHMBOXDB)
        root = xmldata.getroot()
        for entry in root:
            if entry.attrib['type'] != 'song':
                continue

            properties = {}
            for property in entry:
                properties[property.tag] = property.text

            path = urllib.parse.unquote(properties['location'])

            if not path.startswith('file://'):
                raise ValueError('not a file:// URL: ' + path)
            path = path[len('file://'):]

            if path.endswith('.part'):
                continue

            try:
                st = os.stat(path)
            except os.error:
                continue

            # if not os.path.isfile(path):
            if not stat.S_ISREG(st.st_mode):
                continue

            if not path.startswith(self.source):
                continue

            relpath = path[len(self.source):]

            artist = properties['artist']
            if 'album-artist' in properties:
                artist = properties['album-artist']
            album = properties['album']
            title = properties['title']
            duration = int(properties['duration'])
            bitrate = None
            if 'bitrate' in properties:
                bitrate = int(properties['bitrate'])

            info = fileinfo(relpath, st, duration, bitrate)

            if artist not in self.artistDb:
                self.artistDb[artist] = {}
            if album not in self.artistDb[artist]:
                self.artistDb[artist][album] = {}
            self.artistDb[artist][album][title] = info
            self.fileDb[path] = info

    def mayCopy(self, path):
        for nc in self.exclude:
            if (path+'/').startswith(nc.rstrip('/')+'/'):
                return False
        return True

    def addSeen (self, trackpath, srcpath):
        ''' Mark file as seen '''
        if trackpath in self.seenFiles:
            print ('Duplicate!')
            print ('path1:', self.seenFiles[trackpath])
            print ('path2:', srcpath)
            return True # error

        self.seenFiles[trackpath] = srcpath

    def ensureDir(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def doSync(self):
        self.toConvert = []

        for tp in sorted(self.seenFiles.keys()):
            path = self.seenFiles[tp]
            ext = os.path.splitext(path)[1].lower()
            if ext.lower() in LOSSLESSFORMATS:
                destpath = os.path.join(self.dest, tp)+self.lossy_ext
                if os.path.isfile(destpath):
                    # warning, this only updates metadata. If the music itself
                    # is changed, that won't be copied.
                    # dest may be a bit off that's why there is a 2 second
                    # margin
                    if os.stat(path).st_mtime > os.stat(destpath).st_mtime+2:
                        self.copyTags(path, destpath, log=True)
                    continue
                self.toConvert.append([path, destpath])
            else:
                if self.musicDirs.get(os.path.dirname(tp), None) != os.path.dirname(path):
                    # ignore dirs that don't contain music
                    continue

                destpath = os.path.join(self.dest, tp)+ext

                if ext.lower() == '.mp3' and os.path.isfile(destpath + self.lossy_ext):
                    # transcoded MP3

                    if os.stat(path).st_mtime > os.stat(destpath + self.lossy_ext).st_mtime:
                        # *ASSUME* it's just metadata that got updated...
                        self.copyTags(path, destpath + self.lossy_ext, log=True)

                    continue

                if os.path.isfile(destpath):
                    # check whether the source path got replaced
                    if not os.path.samefile(path, destpath):
                        if os.stat(path).st_mtime + 2 >= os.stat(destpath).st_mtime:
                            print ('replaced:', path)
                            os.remove(destpath)
                            os.link(path, destpath)
                        else:
                            print ('replaced dest:', path)
                            os.remove(path)
                            os.link(destpath, path)
                    continue
                self.ensureDir(destpath)
                print ('new:', destpath)
                os.link(path, destpath)


    def copyTags (self, srcFile, dstFile, log=False, albumArtistWorkaround=False):
        tags = {
        }
        if srcFile.lower().endswith('.flac'):
            src = mutagen.flac.FLAC(srcFile)
            for tag in src:
                tags[tag] = src[tag]
        elif srcFile.lower().endswith('.mp3'):
            src = mutagen.easyid3.EasyID3(srcFile)
            for tag in src:
                if tag == 'performer':
                    tags['albumartist'] = src['performer']
                else:
                    tags[tag] = src[tag]
        elif srcFile.lower().endswith('.m4a'):
            src = mutagen.easymp4.EasyMP4(srcFile)
            for tag in src:
                tags[tag] = src[tag]
        else:
            raise RuntimeError('Unsupported file: ' + srcFile)

        changed = False

        # Some media players don't support the album artist tag, or only
        # partially.
        if albumArtistWorkaround:
            if 'albumartist' in tags:
                tags['artist'] = tags['albumartist']
                del tags['albumartist']

        if dstFile.endswith('.part'):
            dstExt = os.path.splitext(dstFile[:-len('.part')])[1]
        else:
            dstExt = os.path.splitext(dstFile)[1]

        if dstExt == '.opus':
            dst = mutagen.oggopus.OggOpus(dstFile)
            for tag in tags:
                if tags[tag] != dst.get(tag):
                    if log:
                        print ('changed: %s (%r => %r)' % (tag, dst.get(tag), tags[tag]))
                    dst[tag] = tags[tag]
                    changed = True

        elif dstExt == '.m4a':
            dst = mutagen.easymp4.EasyMP4(dstFile)
            for tag in tags:
                if tag in {'tracknumber', 'discnumber'} and canonicalIndex(tags[tag][0]) == canonicalIndex(dst.get(tag)):
                    continue
                if tag in mutagen.easymp4.EasyMP4.Set and tags[tag] != dst.get(tag):
                    if log:
                        print ('changed: %s (%r => %r)' % (tag, dst.get(tag), tags[tag]))
                    dst[tag] = tags[tag]
                    changed = True
        elif dstExt in {'.ogg', '.oga'}:
            # See:
            # http://age.hobba.nl/audio/mirroredpages/ogg-tagging.html
            # I couldn't find any official source of tags.
            allowedTags = {
                'title',
                'artist',
                'albumartist',
                'album',
                'tracknumber',
                'discnumber',
                'date',
                'genre'
                'copyright',
                'encodedby',
                'performer',
            }
            dst = mutagen.File(dstFile)
            for tag in tags:
                if not tag in allowedTags:
                    continue
                if tags[tag] != dst.get(tag.upper()):
                    if log:
                        print ('  changed: %s (%r => %r)' % (tag, dst.get(tag), tags[tag]))
                    dst[tag.upper()] = tags[tag]
                    changed = True
            for tag in dst:
                if not tag.lower() in tags:
                    if log:
                        print ('  deleted: %s (%r)' % (tag, dst.get(tag)))
                    del dst[tag]
                    changed = True
        else:
            raise RuntimeError('unrecognized file: ' + dstFile)

        if changed:
            if log:
                print ('cp tags:', dstFile)
            dst.save()

    def findOld (self):
        self.oldFiles = []
        for directory, dirs, files in os.walk(self.dest, topdown=False):
            dirs.sort()
            files.sort()
            for fn in files:
                path      = os.path.join(directory, fn)
                relpath   = os.path.relpath(path, self.dest)
                trackpath, ext = os.path.splitext(relpath)

                if path.startswith('/home/ayke/Music-portable/.sync/'):
                    continue

                # transcoded MP3 files
                if relpath.lower().endswith('.mp3' + self.lossy_ext):
                    trackpath = os.path.splitext(trackpath)[0]
                elif ext.lower() == '.mp3' and os.path.isfile(path + self.lossy_ext):
                    self.oldFiles.append(path)
                    continue

                if path.startswith('/home/ayke/Music-portable/.sync/'):
                    continue

                if path.startswith('/home/ayke/Music-portable/.stignore'):
                    continue

                if trackpath not in self.seenFiles \
                        or os.path.dirname(relpath) not in self.musicDirs:
                    self.oldFiles.append(path)

    def mayClearOld(self):
        # first remove all old files
        if self.oldFiles:
            print ('Files to remove:')
            for path in self.oldFiles:
                print (' * ', path)
            if not self.confirmRemove or input('Remove [y/N]? ').strip().lower() == 'y':
                for path in self.oldFiles:
                    # file could have been removed in the meantime
                    if os.path.isfile(path):
                        os.remove(path)
                    else:
                        print ('Gone:\t' + path)
                print ('Removing done.')
            else:
                # do not remove empty directories when the answer is no
                return

        # now remove empty dirs
        # It's a bit redundant, but it's the easiest option and shouldn't have
        # such a performance impact
        for directory, dirs, files in os.walk(self.dest, topdown=False):
            dirs.sort()
            for dn in dirs:
                path = os.path.join(directory, dn)
                if path.find('/.sync') >= 0:
                    # Don't touch the .sync folder.
                    # I think because of the topdown parameter, we can't just
                    # modify the 'dirs' list.
                    continue
                try:
                    os.rmdir(path)
                except OSError as e:
                    # ENOTEMPTY is expected, that just means the directory isn't
                    # empty and thus shouldn't be removed (it probably contains
                    # songs)
                    if e.errno != errno.ENOTEMPTY:
                        raise # some other error
                else:
                    print ('removed empty dir:', path)


    def convertLossless(self):
        ''' Convert all lossless FLAC files to a lossy format, reducing quality
            but which is unaudible to me (~65k stereo)
        '''
        if not self.toConvert:
            # nothing to convert
            return

        files = {}

        total_bytes = 0

        for inpath, outpath in self.toConvert:
            self.ensureDir(outpath)
            duration = mutagen.flac.FLAC(inpath).info.length
            files[inpath] = {
                'outpath': outpath,
                'duration': duration,
            }
            total_bytes += os.stat(inpath).st_size

        if not files:
            return

        print ('\nTo convert: %dMB FLAC' % (total_bytes/1024/1024))
        self.transcodeAll(files)


    def transcodeLossy(self):
        if self.minimum_transcode_bitrate == 0:
            files, total_bytes = self.getAllMP3s()
        else:
            files, total_bytes = self.getHighBitrateMP3s()

        if not files:
            return

        print ('\nTo convert: %dMB MP3' % (total_bytes/1024/1024))
        self.transcodeAll(files)

    def getAllMP3s(self, path=None, files=None):
        mp3files = {}
        total_bytes = 0

        for directory, dirs, files in os.walk(self.source):
            if '.sync' in dirs:
                dirs.remove('.sync')

            dirs.sort()
            files.sort()

            for fn in files:
                path = os.path.join(directory, fn)
                if fn.lower().endswith('.mp3'):
                    if not self.mayTranscode(path):
                        continue

                    outpath = self.dest + path[len(self.source):] + self.lossy_ext
                    if os.path.isfile(outpath):
                        continue

                    st = os.stat(path)
                    total_bytes += st.st_size

                    duration = st.st_size / 40 # for ~320kbps
                    if path in self.getFileDB():
                        duration = self.fileDb[path].duration

                    mp3files[path] = {
                        'outpath': outpath,
                        'duration': duration,
                    }

        return mp3files, total_bytes

    def getHighBitrateMP3s(self):
        total_bytes = 0

        # {path: {'duration': ..., 'outpath': ...}}
        files = {}
        for path, info in self.getFileDB().items():

            if not path.lower().endswith('.mp3'):
                continue

            if not self.mayTranscode(path):
                continue

            outpath = self.dest + info.relpath + self.lossy_ext
            if os.path.isfile(outpath):
                continue

            if info.bitrate < self.minimum_transcode_bitrate:
                continue

            total_bytes += info.stat.st_size

            files[path] = {
                'outpath': outpath,
                'duration': info.duration,
            }

        return files, total_bytes

    def mayTranscode(self, path):
        for nt in self.exclude:
            if path.startswith(nt):
                return False
        for nt in self.excludeTranscode:
            if path.startswith(nt):
                return False
        return True

    def transcodeAll(self, files):
        if not files:
            return

        duration_total = sum(map(lambda o: o['duration'], files.values()))
        duration_started = 0
        current_durations = []

        def worker(queue):
            while True:
                task = queue.get()
                if task is None:
                    break
                queue.task_done()
                inpath, outpath = task
                self.transcodeFile(inpath, outpath)

        queue = Queue(1)
        start = time.time()

        threads = []
        for i in range(MAXPROCS):
            t = threading.Thread(target=worker, args=(queue,))
            t.daemon = True
            t.start()
            threads.append(t)

        # transcode all MP3s
        statusLine = ''
        for path in sorted(files.keys()):
            properties = files[path]
            outpath = properties['outpath']

            queue.put([path, outpath], block=True)
            queue.join()
            print (' '*len(statusLine)+'\r'+path)

            duration_started += properties['duration']
            current_durations = (current_durations + [properties['duration']])[-4:]

            now = time.time()

            # approximate how much of the durations is finished
            duration_done = duration_started - sum(current_durations[math.ceil(len(current_durations)/2):])

            speed = duration_done/(now-start) # music-seconds per time-second
            remaining_time = (duration_total-duration_done)/speed
            percent = duration_done*100/duration_total
            statusLine = '%.2f%% %dx (remaining: %d:%02d)' % (percent, speed, remaining_time//60, remaining_time%60)
            print (statusLine, end='\r')
            sys.stdout.flush()

        for i in range(len(threads)):
            queue.put(None, block=True)

        # wait until all threads are finished
        for t in threads:
            t.join()

        total_time = time.time()-start
        avg_speed  = duration_total/total_time
        # this also overwrites the progress indicator
        print ('\rFinished in %d:%02d (avg. speed %.1fx)' % (total_time//60, total_time%60, avg_speed))

    def transcodeFile(self, inpath, outpath):
        if not outpath.endswith(self.lossy_ext):
            raise ValueError('Unrecognized output file: ' + outpath)

        infile = open(inpath, 'a')
        try:
            lockf(infile, LOCK_EX|LOCK_NB)
        except IOError as e:
            if e.errno == errno.EWOULDBLOCK:
                infile.close()
                return
            raise

        destpath = outpath[:-len(self.lossy_ext)]

        tmppath = outpath + '.part'

        wavpath = tmpname(os.path.basename(inpath + '.wav'))

        try:
            if inpath.lower().endswith('.mp3'):
                # decode MP3
                # XXX --no-resync?
                output = subprocess.check_output(['mpg123', '--quiet', '-w', wavpath, inpath], stderr=subprocess.STDOUT)
                if output:
                    sys.stderr.write(output.decode())
                    if os.path.isfile(wavpath):
                        os.remove(wavpath)
                    lockf(infile, LOCK_UN)
                    infile.close()
                    return
            elif inpath.lower().endswith('.flac'):
                if subprocess.call(['flac', '-fds', inpath, '-o', wavpath]):
                    if os.path.isfile(wavpath):
                        os.remove(wavpath)
                    lockf(infile, LOCK_UN)
                    infile.close()
            else:
                raise RuntimeError('unknown input file type: '+inpath)

            parentdir = os.path.dirname(destpath)
            os.makedirs(parentdir, exist_ok=True)

            # Transcode!
            if self.lossy_ext == '.opus':
                subprocess.check_call(['opusenc', '--bitrate', OPUS_QUALITY, wavpath, tmppath], stderr=PIPE)
            elif self.lossy_ext == '.m4a':
                subprocess.check_call(['neroAacEnc', '-q', AAC_QUALITY, '-if', wavpath, '-of', tmppath], stderr=PIPE)
            else:
                raise RuntimeError('unknown output file type: '+self.lossy_ext)

            # copy tags
            self.copyTags(inpath, tmppath)

            # move to final position
            os.rename(tmppath, outpath)

            if os.path.isfile(destpath):
                # remove bigger and duplicate file
                os.remove(destpath)

        finally:
            # remove temporary WAV file - if it's there
            if os.path.isfile(wavpath):
                os.remove(wavpath)

            if os.path.isfile(tmppath):
                os.remove(tmppath)

        lockf(infile, LOCK_UN)
        infile.close()

def tmpname(suffix):
    global tmp_number

    # due to the GIL, we don't have to care about atomicity
    tmp_number += 1
    return '%s/musicsync-%d-%s' % (TMPDIR, tmp_number, suffix)

def getInfo(path):
    return json.loads(subprocess.check_output(['ffprobe', '-loglevel', 'error', '-i', path, '-print_format', 'json', '-show_streams']))

def canonicalIndex(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = [value]
    return list(map(lambda v: '/'.join(map(str, map(int, v.split('/')))), value))
