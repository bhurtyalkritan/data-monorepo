import '@mui/material/Grid';
declare module '@mui/material/Grid' {
  interface GridProps {
    size?: number | 'auto' | 'grow' | { xs?: number | 'auto' | 'grow'; sm?: number | 'auto' | 'grow'; md?: number | 'auto' | 'grow'; lg?: number | 'auto' | 'grow'; xl?: number | 'auto' | 'grow' };
  }
}
