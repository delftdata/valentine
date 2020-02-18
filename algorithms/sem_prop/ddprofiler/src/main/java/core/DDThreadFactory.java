package core;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ThreadFactory;

public class DDThreadFactory implements ThreadFactory {

  final Queue<String> threadNames;

  public DDThreadFactory(List<String> uniqueThreadNames) {
    threadNames = new ArrayDeque<>();
    for (String s : uniqueThreadNames) {
      threadNames.add(s);
    }
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(r, threadNames.poll());
    return t;
  }
}
