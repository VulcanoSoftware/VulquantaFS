import { TextField, TextFieldProps } from "@mui/material";

interface ConfigTextFieldProps extends Omit<TextFieldProps, "size"> {
  width?: string | number;
}

export function ConfigTextField({
  width = 300,
  sx,
  ...props
}: ConfigTextFieldProps) {
  return (
    <TextField
      size="small"
      sx={{ width: { xs: "100%", sm: width }, maxWidth: "100%", ...sx }}
      {...props}
    />
  );
}
