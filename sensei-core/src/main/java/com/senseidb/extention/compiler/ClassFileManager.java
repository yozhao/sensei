package com.senseidb.extention.compiler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.SecureClassLoader;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;

public class ClassFileManager extends ForwardingJavaFileManager {
  private JavaClassObject jclassObject;

  public ClassFileManager(StandardJavaFileManager
                              standardManager) {
    super(standardManager);
  }

  @Override
  public ClassLoader getClassLoader(Location location) {
    return new SecureClassLoader() {
      @Override
      protected Class<?> findClass(String name)
          throws ClassNotFoundException {
        byte[] b = jclassObject.getBytes();
        return super.defineClass(name, jclassObject
            .getBytes(), 0, b.length);
      }
    };
  }

  @Override
  public JavaFileObject getJavaFileForOutput(Location location,
                                             String className, JavaFileObject.Kind kind, FileObject sibling)
      throws IOException {
    jclassObject = new JavaClassObject(className, kind);
    return jclassObject;
  }
  static public class JavaClassObject extends SimpleJavaFileObject {

    protected final ByteArrayOutputStream bos =
        new ByteArrayOutputStream();

    public JavaClassObject(String name, Kind kind) {
      super(URI.create("string:///" + name.replace('.', '/')
                       + kind.extension), kind);
    }

    public byte[] getBytes() {
      return bos.toByteArray();
    }

    @Override
    public OutputStream openOutputStream() throws IOException {
      return bos;
    }
  }
}
