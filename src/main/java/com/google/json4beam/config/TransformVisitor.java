package com.google.json4beam.config;

import java.lang.reflect.Method;

/** TransformVisitor defines interface with functions to visit Beam transform classes */
public interface TransformVisitor {
  void visit(Class<?> clazz);

  void visit(Method method);
}
