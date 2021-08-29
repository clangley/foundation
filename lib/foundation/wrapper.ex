defmodule Foundation.Wrapper do
  defmacro deftx({fn_name, _ctx, fn_args}, do: do_block) do
    without_tx =
      Enum.filter(fn_args, fn {var, _ctx, _value} ->
        var != :tx
      end)

    quote do
      def unquote(String.to_atom("#{fn_name}!"))(unquote_splicing(without_tx)) do
        Foundation.trans(Foundation.db(), fn var!(tx) ->
          unquote(do_block)
        end)
      end

      def unquote(fn_name)(unquote_splicing(fn_args)) do
        unquote(do_block)
      end
    end
  end
end
