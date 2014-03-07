# Written by Alexander Starr, student ID 00567613

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class Apache(MRJob):

    def mapper1(self, _, line):
        # First split the line by spaces, not ideal but good enough for our needs.
        elems = line.strip().split()
        # Get the IP address.
        client = elems[0]
        # Now calculate the UTC offset in seconds.
        offset = elems[4][:-1]
        sign = float(offset[0:1] + '1')
        offset_sec = sign * float(offset[1:3]) * 3600 + sign * float(offset[3:]) * 60
        # Finally, find the Unix timestamp (with UTC offset) for the log.
        time_str = elems[3][1:]
        timestamp = time.mktime(time.strptime(time_str, '%d/%b/%Y:%H:%M:%S')) + offset_sec
        yield (client, timestamp)

    def reducer1(self, client, timevector):
        s_times = sorted(list(timevector))
        stlen = len(s_times)
        if stlen == 1:
            return
        time_total = 0.0
        for i in range(1, stlen):
            time_total += s_times[i] - s_times[i-1]
        avetime = time_total / float(stlen)
        yield (client, avetime)

    def mapper2(self, client, avetime):
        yield (client, avetime)

    def reducer2(self, client, avetime):
        this_avg = list(avetime)[0]
        try:
            max_avg = self.MAXAVG
        except:
            max_avg = 0.0
            self.CLIENT = ''

        if this_avg > max_avg:
            self.MAXAVG = this_avg
            self.CLIENT = client
        return

    def reducer_final(self):
        yield (str(self.CLIENT), str(self.MAXAVG))

    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1),
                MRStep(mapper=self.mapper2, reducer=self.reducer2, reducer_final=self.reducer_final)]

if __name__ == '__main__':
    Apache.run()
