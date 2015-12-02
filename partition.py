import itertools


class Partition(object):
  def __init__(self, rdd_id, partition_id, content):
    self.content = content
    self.id = partition_id
    self.rdd_id = rdd_id

  def get_content(self):
    self.content, r = itertools.tee(self.content)
    return list(r)

  def get(self):
    return {
      'id': self.id,
      'rdd_id': self.rdd_id,
      'content': list(self.get_content())
    }

if __name__ == '__main__':
  p = Partition(1,1,"hello,world")
  print list(p.get_content())