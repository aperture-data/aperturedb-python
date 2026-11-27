from typing import Callable, List, Optional, Dict, Tuple, Iterator
import logging

logger = logging.getLogger(__name__)

# TODO: Elevate this to a shared library


class SymbolicBatcher:
    """This class is a lightweight way to run batches of commands
    to ApertureDB. It handles symbolic references and
    automatically flushes the batch when it reaches a certain size.
    It allows for a prolog and epilog to be run before and after
    the batch, respectively. The prolog and epilog are functions
    that return an additional list of commands to be run at the start
    and end of the batch. The prolog and epilog are run in the
    context of the batch, so they can use the same symbolic references.
    """

    def __init__(
        self,
        execute_query: Callable[[List[dict], List[bytes]], Tuple[List[dict], List[bytes]]],
        batch_size: int = 100,
        prolog: Optional[Callable[[], List[dict]]] = None,
        epilog: Optional[Callable[[], List[dict]]] = None,
    ):
        self.execute_query = execute_query
        self.batch_size = batch_size
        self.prolog_fn = prolog or (lambda: [])
        self.epilog_fn = epilog or (lambda: [])

        self._commands: List[dict] = []
        self._response_handlers: List[Tuple[int, int, Callable]] = []
        self._blobs: List[bytes] = []
        self._ref_map: dict[str, int] = {}
        self._ref_counter = 1
        self._batch_started = False

    def empty(self) -> bool:
        """Returns True if the batch is empty."""
        return not self._commands

    def add(self,
            items: Iterator[dict],
            blobs: Optional[Iterator[bytes]] = [],
            response_handler=None):

        self._batch_started = True

        if response_handler is not None:
            # (start, length, response_handler)
            self._response_handlers.append((len(self._commands), len(resolved),
                                            response_handler))
        self._commands.extend(items)

        if blobs is not None:
            self._blobs.extend(blobs)

        if self._count_commands() >= self.batch_size:
            self.flush()

    def flush(self):
        if self.empty():
            return

        logger.info("Flushing %d commands", len(self._commands))

        commands = []
        for proto in self.prolog_fn():
            commands.append(self._resolve_refs_in_command(proto))
        commands_start = len(commands)

        for proto in self._commands:
            commands.append(self._resolve_refs_in_command(proto))

        for proto in self.epilog_fn():
            commands.append(self._resolve_refs_in_command(proto))

        results, blobs = self.execute_query(commands, self._blobs)

        for start, length, response_handler in self._response_handlers:
            sub_results = results[commands_start +
                                  start:commands_start + start + length]
            assert len(sub_results) == length
            result_blobs = None
            for result in sub_results:
                new_start = len(result_blobs)
                if "blobs_start" in result:
                    result_blobs.extend(
                        blobs[result["blobs_start"]: result["blobs_start"] + result["returned"]])
                    result["blobs_start"] = new_start
                elif "blob_index" in result:
                    result_blobs.append([blobs[result["blob_index"]]])
                    result["blob_index"] = new_start
            response_handler(result, result_blobs)

        self._commands.clear()
        self._blobs.clear()
        self._response_handlers.clear()
        self._ref_map.clear()
        self._ref_counter = 1
        self._batch_started = False

        logger.info("Flushed %d commands", len(commands))

    def _assign_ref(self, obj: dict, field: str) -> None:
        if field not in obj:
            return
        if not isinstance(obj[field], str):
            logger.error(
                f"Numeric ref ({obj}, {field}) not allowed")
            raise ValueError(
                f"Numeric ref ({obj}, {field}) not allowed")
        symbol = obj[field]
        ref = self._ref_counter
        self._ref_counter += 1
        self._ref_map[symbol] = ref
        obj[field] = ref

    def _lookup_ref(self, obj: dict, field: str) -> None:
        if field not in obj:
            return
        if not isinstance(obj[field], str):
            logger.error(
                f"Numeric ref ({obj}, {field}) not allowed")
            raise ValueError(
                f"Numeric ref ({obj}, {field}) not allowed")
        symbol = obj[field]
        if symbol not in self._ref_map:
            logger.error(
                f"Symbolic reference '{symbol}' not assigned yet")
            raise ValueError(f"Symbolic reference '{symbol}' not assigned yet")
        obj[field] = self._ref_map[symbol]

    def _resolve_refs_in_command(self, command):
        # TODO: Deep copy?
        logger.debug("Resolving refs in command: %s", command)
        assert isinstance(command, dict)
        command_name = next(iter(command))
        command_body = command[command_name]

        self._assign_ref(command_body, "_ref")
        for x in ["is_connected_to", "connect"]:
            if x in command_body:
                self._lookup_ref(command_body[x], "ref")
        if command_name == "AddConnection":
            for x in ["src", "dst"]:
                self._lookup_ref(command_body, x)
        return command

    def _count_commands(self):
        """Returns the number of commands in the batch.

        This is abstracted in case we want to, say, count only Add commands.
        """
        return len(self._commands)
