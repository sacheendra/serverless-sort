

BUFSIZE = 64 * 1024
def copyfileobj(fsrc, fdst):
	# shutil.copyfileobj creates a lot of garbage creating multiple buffers. This doesn't
	buf = bytearray(BUFSIZE)
	while (bytes_read := fsrc.readinto(buf)) != 0:
		fdst.write(memoryview(buf)[:bytes_read])