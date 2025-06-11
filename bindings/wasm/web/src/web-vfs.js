export class VFS {
  constructor() {
    return self.vfs;
  }

  open(path, flags) {
    return self.vfs.open(path);
  }

  close(fd) {
    return self.vfs.close(fd);
  }

  pread(fd, buffer, offset) {
    return self.vfs.pread(fd, buffer, offset);
  }

  pwrite(fd, buffer, offset) {
    return self.vfs.pwrite(fd, buffer, offset);
  }

  size(fd) {
    return self.vfs.size(fd);
  }

  truncate(fd, size) {
    return self.vfs.truncate(fd,size)
  }

  sync(fd) {
    return self.vfs.sync(fd);
  }
}
