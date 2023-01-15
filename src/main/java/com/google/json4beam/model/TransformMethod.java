package com.google.json4beam.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.json4beam.exceptions.TransformServiceException;
import com.google.common.flogger.GoogleLogger;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * TransformMethod represents a method that can be called to initialize a PTransform object
 */
public class TransformMethod extends TransformObject {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  // the class this method belongs to
  //@JsonIgnore
  private TransformClass owner;
  private List<TransformClass> arguments = new ArrayList<TransformClass>();
  private TransformClass returns;
  private boolean isStatic = false;

  public TransformMethod() {
  }

  public TransformMethod(String name, String description) {
    super(name, description);
  }

  public TransformMethod(String name, String description, TransformClass owner) {
    super(name, description);
    this.owner = owner;
  }

  public boolean isStatic() {
    return isStatic;
  }

  public void setStatic(boolean aStatic) {
    isStatic = aStatic;
  }

  public TransformClass getOwner() {
    return owner;
  }

  public List<TransformClass> getArguments() {
    return arguments;
  }

  public TransformClass getReturns() {
    return returns;
  }

  public void setOwner(TransformClass owner) {
    this.owner = owner;
  }

  public void setArguments(List<TransformClass> arguments) {
    this.arguments = arguments;
  }

  public void setReturns(TransformClass returns) {
    this.returns = returns;
  }

  public void addArgument(TransformClass argument) {
    this.arguments.add(argument);
  }

  @JsonIgnore
  public Class<?>[] getArgumentClasses() {
    Class<?>[] classes = new Class<?>[arguments.size()];
    int index = 0;
    for (TransformClass clazz : arguments) {
      classes[index++] = clazz.getClazz();
    }
    return classes;
  }

  public Object invoke(Map<String, Object> properties) {
    Class<?> clazz = owner.getClazz();
    Class<?>[] classes = getArgumentClasses();
    try {
      Method method = clazz.getMethod(getName(), classes);
      Object[] values = getValues(properties);
      if (values.length == classes.length) {
        return method.invoke(clazz, values);
      } else {
        throw new TransformServiceException("Missing required properties.");
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new TransformServiceException("Unhandled exception", e);
    }
  }

  public Object invoke(Object object, Map<String, Object> properties) {
    if (isStatic()) {
      throw new TransformServiceException("Static method can not be called on an object.");
    }
    Object newObject = null;
    Class<?>[] classes = getArgumentClasses();
    logger.atInfo().log("Method: %s has %d arguments.", getName(), classes.length);
    for (int i = 0; i < classes.length; i++) {
      logger.atInfo().log("Method: %s has argument %s.", getName(), arguments.get(i).getName());
    }

    try {
      Method method = object.getClass().getMethod(getName(), classes);
      if (classes.length > 0) {
        Object[] values = getValues(properties);
        logger.atInfo().log("Method: %s has %d argument values.", getName(), values.length);
        for (int i = 0; i < values.length; i++) {
          logger.atInfo().log(
              "Method: %s has argument value %s=%s.",
              getName(), classes[i].getName(), values[i].toString());
        }
        if (values.length == classes.length) {
          logger.atInfo().log("Invoking method: %s", method.getName());
          if(method.getReturnType() == void.class) {
            method.invoke(object, values);
          }else{
            newObject = method.invoke(object, values);
          }
        }
      } else {
        logger.atInfo().log("Check if Method: %s need to invoke.", getName());
        Object pv = properties.get(getName());
        if (pv != null) {
          logger.atInfo().log("Method: %s enabled: %s", getName(), pv.toString());
          boolean pvboolean = Boolean.valueOf(pv.toString());
          if (pvboolean) {
            logger.atInfo().log("Invoking method: %s", getName());
            if(method.getReturnType() == void.class) {
              method.invoke(object);
            }else{
              newObject = method.invoke(object);
            }
          }
        } else {
          logger.atInfo().log("Method: %s enabled: false", getName());
        }
      }
      if(newObject!=null){
        return newObject;
      }
      return object;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new TransformServiceException("Unhandled exception", e);
    }
  }

  @JsonIgnore
  private Object[] getValues(Map<String, Object> properties) {
    List<Object> values = new ArrayList<>();
    for (TransformClass clazz : arguments) {
      String propertyName = clazz.getName();
      if (properties.containsKey(propertyName)) {
        values.add(properties.get(propertyName));
      }
    }
    return values.toArray();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TransformMethod)) {
      return false;
    }
    TransformMethod that = (TransformMethod) o;
    return getName().equals(that.getName()) && isStatic == that.isStatic && Objects.equals(arguments,
        that.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), arguments, isStatic);
  }
}
