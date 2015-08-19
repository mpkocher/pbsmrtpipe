C=/pbi/dept/secondary/testdata/git_sym_cache

old-pbsmrtpipe-m140905_042212_sidney_c100564852550000001823085912221377_s1_X0.1.bax.h5-6f490d31b9dbf67091849ae8ae922758241bc971:
	cp -f $C/$@ $@
old-pbsmrtpipe-test.bam-24749ac19bd8506258d003cc0dbc5c31cb8edc4c:
	cp -f $C/$@ $@
old-pbsmrtpipe-small.fasta-6cb3c513ec3fa52f9527d4ba59174232345369ca:
	cp -f $C/$@ $@
old-pbsmrtpipe-small.fastq-96f4160d32b8e0546cec400a94a2dfd01de6486b:
	cp -f $C/$@ $@
old-pbsmrtpipe-example.fasta.sa-3f240b63033902f6f87358fc6d200097afdddd66:
	cp -f $C/$@ $@

pbsmrtpipe-m140905_042212_sidney_c100564852550000001823085912221377_s1_X0.1.bax.h5-6f490d31b9dbf67091849ae8ae922758241bc971:
	curl -L -o $@.tmp https://www.dropbox.com/s/4matqyubwtl5buj/pbsmrtpipe-m140905_042212_sidney_c100564852550000001823085912221377_s1_X0.1.bax.h5-6f490d31b9dbf67091849ae8ae922758241bc971?dl=0 && mv -f $@.tmp $@
pbsmrtpipe-test.bam-24749ac19bd8506258d003cc0dbc5c31cb8edc4c:
	curl -L -o $@.tmp https://www.dropbox.com/s/gw6hchvrg442otl/pbsmrtpipe-test.bam-24749ac19bd8506258d003cc0dbc5c31cb8edc4c?dl=0 && mv -f $@.tmp $@
pbsmrtpipe-small.fasta-6cb3c513ec3fa52f9527d4ba59174232345369ca:
	curl -L -o $@.tmp https://www.dropbox.com/s/dwkobtxdzh7hr00/pbsmrtpipe-small.fasta-6cb3c513ec3fa52f9527d4ba59174232345369ca?dl=0 && mv -f $@.tmp $@
pbsmrtpipe-small.fastq-96f4160d32b8e0546cec400a94a2dfd01de6486b:
	curl -L -o $@.tmp https://www.dropbox.com/s/tmlyc4lknww6js3/pbsmrtpipe-small.fastq-96f4160d32b8e0546cec400a94a2dfd01de6486b?dl=0 && mv -f $@.tmp $@
pbsmrtpipe-example.fasta.sa-3f240b63033902f6f87358fc6d200097afdddd66:
	curl -L -o $@.tmp https://www.dropbox.com/s/eqwy42kbvu6fndr/pbsmrtpipe-example.fasta.sa-3f240b63033902f6f87358fc6d200097afdddd66?dl=0 && mv -f $@.tmp $@
