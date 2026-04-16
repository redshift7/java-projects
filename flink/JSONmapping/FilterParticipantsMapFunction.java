package com.example.flink.jsonmapping;

import java.util.Arrays;
import org.apache.flink.api.common.functions.RichMapFunction;
import com.example.classes.messaging.InputMessage;
import com.example.classes.messaging.Position;

public class FilterParticipantsMapFunction extends RichMapFunction<InputMessage, InputMessage>
{
    public InputMessage map(InputMessage input)
    {
        InputMessage output = new InputMessage();
        output.Timestamp = input.Timestamp;
        output.pos = Arrays.stream(input.pos)
                .filter(p -> (p.X != null && p.Y != null)).toArray(Position[]::new);

        return output;
    }
}

