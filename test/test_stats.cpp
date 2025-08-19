
#include <ygm/comm.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  assert(world.stats().get_async_count() == 0);

  int num_messages = 5;
  if (world.rank0()) {
    for (int i = 0; i < num_messages; ++i) {
      for (int j = 1; j < world.size(); ++j) {
        world.async(j, []() {});
      }
    }
  } else {
    for (int i = 0; i < num_messages; ++i) {
      world.async(0, []() {});
    }
  }

  world.barrier();

  if (world.rank0()) {
    YGM_ASSERT_RELEASE(world.stats().get_async_count() ==
                       (world.size() - 1) * num_messages);
    YGM_ASSERT_RELEASE(world.stats().get_rpc_count() ==
                       (world.size() - 1) * num_messages);
  } else {
    YGM_ASSERT_RELEASE(world.stats().get_async_count() == num_messages);
    YGM_ASSERT_RELEASE(world.stats().get_rpc_count() == num_messages);
  }
  return 0;
}
