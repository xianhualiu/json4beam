package com.google.json4beam.config;

import com.google.json4beam.model.TransformClass;
import com.google.json4beam.model.TransformMethod;
import com.google.common.flogger.GoogleLogger;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * TransformConfigVisitor is an implementation of TransformVisitor to visit Beam transform classes
 * and generate configurations.
 */
public class TransformConfigVisitor implements TransformVisitor {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final Class<?> PRIMITIVE_CLASSES[] =
      new Class<?>[]{String.class, Number.class, Boolean.class, Character.class, List.class};
  Map<String, TransformClass> configMap = new HashMap<>();

  @Override
  public void visit(Class<?> clazz) {
    // logger.atInfo().log("Visiting %s", clazz.getCanonicalName());
    if (PTransform.class.isAssignableFrom(clazz)) {
      // logger.atInfo().log("Found Transform: %s", clazz.getCanonicalName());
      for (Constructor<?> c : clazz.getConstructors()) {

        if (c.getParameterCount() > 0) {
          continue;
        }

        // logger.atInfo().log("Found Constructor: %s", c.getName());
        String className = clazz.getCanonicalName();
        String name = getUnitName(clazz);
        // logger.atInfo().log("Found transform class with name: %s", name);
        TransformClass config = configMap.get(name);
        if (config == null) {
          config = new TransformClass(name, className, className);
        }
        // set type
        if (name.indexOf("Read") != -1) {
          config.setType("input");
        } else if (name.indexOf("Write") != -1) {
          config.setType("output");
        } else {
          config.setType(name);
        }
        configMap.put(name, config);
      }
    }
    for (Method method : clazz.getDeclaredMethods()) {
      // logger.atInfo().log("Method %s", method.getName());
      if (Modifier.isPublic(method.getModifiers())) {
        // logger.atInfo().log("Visiting method %s", method.getName());
        visit(method);
      }
    }
    for (Class<?> clazzClass : clazz.getDeclaredClasses()) {
      visit(clazzClass);
    }
  }

  @Override
  public void visit(Method method) {
    // logger.atInfo().log("Visiting method: %s", method.getName());
    if (PTransform.class.isAssignableFrom(method.getReturnType())) {
      String className = method.getReturnType().getCanonicalName();
      String name = getUnitName(method.getReturnType());

      if (Modifier.isStatic(method.getModifiers())) {
        // this is a building method
        TransformClass config = configMap.get(name);
        if (config == null) {
          config = new TransformClass(name, className, className);
        }
        // set type
        if (name.indexOf("Read") != -1) {
          config.setType("input");
        } else if (name.indexOf("Write") != -1) {
          config.setType("output");
        } else {
          config.setType(name);
        }
        // set builder method
        TransformMethod builder = getTransformMethod(method);
        if (builder == null) {
          return;
        }

        Class<?> clazz = method.getDeclaringClass();
        String ownerName = getUnitName(clazz);

        TransformClass owner =
            new TransformClass(ownerName, clazz.getCanonicalName(), clazz.getCanonicalName());
        builder.setOwner(owner);
        // builder.setReturns(config);
        builder.setStatic(true);
        config.setBuilder(builder);
        configMap.put(name, config);
      } else if (Modifier.isPublic(method.getModifiers())) {
        // property method
        TransformClass config = configMap.get(name);
        if (config == null) {
          return;
        }

        TransformMethod attr = getTransformMethod(method);
        if (attr == null) {
          return;
        }

        // attr.setReturns(config);
        attr.setStatic(false);
        // attr.setOwner(config);
        config.addAttribute(attr);
      }
    } else if (PTransform.class.isAssignableFrom(method.getDeclaringClass())) {
      String className = method.getDeclaringClass().getCanonicalName();
      String name = getUnitName(method.getDeclaringClass());
      TransformClass config = configMap.get(name);
      if (config == null) {
        return;
      }
      if (isSetter(method)) {
        TransformMethod attr = getTransformMethodForSetter(method);
        if (attr == null) {
          return;
        }

        // attr.setReturns(config);
        attr.setStatic(false);
        // attr.setOwner(config);
        config.addAttribute(attr);
      }

    }
  }

  private TransformMethod getTransformMethod(Method method) {
    TransformMethod attr = new TransformMethod();
    attr.setName(method.getName());
    attr.setStatic(false);

    for (Parameter param : method.getParameters()) {
      // logger.atInfo().log("Visiting Parameter: %s", param.getName());
      if (!isPrimitive(param.getType())) {
        return null;
      }
      String attrName = param.getName();

      String attrClassName = param.getType().getCanonicalName();
      TransformClass arg = new TransformClass(attrName, attrClassName, attrClassName);
      attr.addArgument(arg);
    }
    return attr;
  }

  private TransformMethod getTransformMethodForSetter(Method method) {
    TransformMethod attr = new TransformMethod();
    attr.setName(method.getName());
    attr.setStatic(false);

    for (Parameter param : method.getParameters()) {
      // logger.atInfo().log("Visiting Parameter: %s", param.getName());
      if (!isPrimitive(param.getType())) {
        return null;
      }
      String attrName = method.getName().substring(3);
      attrName = attrName.substring(0,1).toLowerCase(Locale.ROOT) + attrName.substring(1);
      String attrClassName = param.getType().getCanonicalName();
      TransformClass arg = new TransformClass(attrName, attrClassName, attrClassName);
      attr.addArgument(arg);
    }
    return attr;
  }

  private boolean isPrimitive(Class<?> clazz) {
    for (Class<?> clazzClass : PRIMITIVE_CLASSES) {
      if (clazzClass.isAssignableFrom(clazz)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isSetter(Method method) {
    return Modifier.isPublic(method.getModifiers()) &&
        method.getReturnType().equals(void.class) &&
        method.getParameterTypes().length == 1 &&
        method.getName().matches("^set[A-Z].*");
  }

  public Map<String, TransformClass> getConfigMap() {
    return configMap;
  }

  private String getUnitName(Class<?> clazz) {
    String name = clazz.getSimpleName();
    if (clazz.getEnclosingClass() != null) {
      name = clazz.getEnclosingClass().getSimpleName() + "." + name;
    }
    return name;
  }
}
