package com.senseidb.tools;

import java.io.File;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import proj.zoie.api.ZoieIndexReader;

public class IndexSplitter {

  private final File srcIdx;
  private final File targetDir;
  private final int maxShardId;
  
  public IndexSplitter(File srcIdx,int maxShardId, File targetDir){
    this.srcIdx = srcIdx;
    this.targetDir = targetDir;
    this.maxShardId = maxShardId;
  }
  
  public void splitTo(int[] targetPartitions) throws Exception{
    IndexReader reader = IndexReader.open(FSDirectory.open(srcIdx), false);
    ZoieIndexReader<?> zreader = ZoieIndexReader.open(reader);
    int maxdoc = reader.maxDoc();
    int numdoc = reader.numDocs();
    
    System.out.println("total doccount: "+numdoc);
    System.out.println("total numdel: "+reader.numDeletedDocs());
    
    for (int k : targetPartitions){
      File idxDir = new File(targetDir,"shard"+k);
      for (int i = 0; i< maxdoc; ++i){
        if (zreader.isDeleted(i)) continue;
        long uid = zreader.getUID(i);
        int shard = (int)(uid % maxShardId);
        if (k != shard){
          reader.deleteDocument(i);
        }
      }
      IndexWriterConfig writerConf = new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35));
      IndexWriter writer = new IndexWriter(FSDirectory.open(idxDir), writerConf);
      writer.addIndexes(reader);
      writer.commit();
      writer.close();
      Runtime.getRuntime().exec("cp "+srcIdx.getAbsolutePath()+"/index.directory "+idxDir.getAbsolutePath());
      // verify
      IndexReader tmpReader = IndexReader.open(FSDirectory.open(idxDir));
      ZoieIndexReader<?> zTmpReader = ZoieIndexReader.open(tmpReader);
      System.out.println("verifying shard: "+k+", numdocs: "+tmpReader.numDocs());
      for (int i=0;i<tmpReader.maxDoc(); ++i){
        long uid = zTmpReader.getUID(i);
        int shard = (int)(uid % maxShardId);
        if (shard != k){
          System.out.println("error: "+uid+" did not belong to shard: "+k+", instead it has shard: "+shard);
          break;
        }
      }
      zreader.undeleteAll();
    }
    zreader.close();
    reader.close();
  }
  
  static void usage(){
	  System.out.println("Usage: <src-idx-dir> <max partition id> <target-dir> partitions, e.g. p1,p2,p3...");
  }
  
  public static void main(String[] args) throws Exception{
	File srcDir = null;
	File targetDir = null;
	int maxPartitionId = 0;
	int[] partitions = null;
	try{
	  srcDir = new File(args[0]);
	  maxPartitionId = Integer.parseInt(args[1]);
	  targetDir = new File(args[2]);
	  String partString = args[3];
	  String[] parts = partString.split(",");
	  partitions = new int[parts.length];
	  for (int i = 0; i < partitions.length; ++i){
		  partitions[i] = Integer.parseInt(parts[i]);
	  }
	}
	catch(Exception e){
		usage();
		System.exit(1);
	}
    IndexSplitter splitter = new IndexSplitter(srcDir,maxPartitionId,targetDir);
    splitter.splitTo(partitions);
  }
}
